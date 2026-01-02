import copy
from collections.abc import Iterator
from datetime import datetime
from functools import partial
from typing import Any
from urllib.parse import urlparse

from box_sdk_gen.client import BoxClient
from box_sdk_gen.client import BoxDeveloperTokenAuth
from box_sdk_gen.client import BoxOAuth
from box_sdk_gen.exception import BoxAPIException
from typing_extensions import override

from onyx.configs.app_configs import GOOGLE_DRIVE_CONNECTOR_SIZE_THRESHOLD
from onyx.configs.app_configs import INDEX_BATCH_SIZE
from onyx.connectors.box.doc_conversion import build_slim_document
from onyx.connectors.box.doc_conversion import convert_box_item_to_document
from onyx.connectors.box.doc_conversion import onyx_document_id_from_box_file
from onyx.connectors.box.doc_conversion import PermissionSyncContext
from onyx.connectors.box.file_retrieval import crawl_folders_for_files
from onyx.connectors.box.file_retrieval import get_all_files_in_folder
from onyx.connectors.box.models import BoxCheckpoint
from onyx.connectors.box.models import BoxRetrievalStage
from onyx.connectors.box.models import RetrievedBoxFile
from onyx.connectors.box.models import StageCompletion
from onyx.connectors.exceptions import ConnectorValidationError
from onyx.connectors.exceptions import CredentialExpiredError
from onyx.connectors.exceptions import InsufficientPermissionsError
from onyx.connectors.interfaces import CheckpointedConnectorWithPermSync
from onyx.connectors.interfaces import CheckpointOutput
from onyx.connectors.interfaces import GenerateSlimDocumentOutput
from onyx.connectors.interfaces import NormalizationResult
from onyx.connectors.interfaces import SecondsSinceUnixEpoch
from onyx.connectors.interfaces import SlimConnectorWithPermSync
from onyx.connectors.models import ConnectorFailure
from onyx.connectors.models import ConnectorMissingCredentialError
from onyx.connectors.models import Document
from onyx.connectors.models import EntityFailure
from onyx.indexing.indexing_heartbeat import IndexingHeartbeatInterface
from onyx.utils.logger import setup_logger
from onyx.utils.threadpool_concurrency import ThreadSafeDict

logger = setup_logger()

FOLDERS_PER_CHECKPOINT = 1
BOX_PAGES_PER_CHECKPOINT = 2


def _extract_str_list_from_comma_str(string: str | None) -> list[str]:
    """Extract list of strings from comma-separated string."""
    if not string:
        return []
    return [s.strip() for s in string.split(",") if s.strip()]


def _extract_ids_from_urls(urls: list[str]) -> list[str]:
    """Extract Box folder/file IDs from URLs."""
    ids = []
    for url in urls:
        parsed = urlparse(url)
        # Box URLs can be: https://app.box.com/folder/123456789
        # or https://app.box.com/file/123456789
        path_parts = parsed.path.strip("/").split("/")
        if len(path_parts) >= 2:
            ids.append(path_parts[-1])
    return ids


class BoxConnector(
    SlimConnectorWithPermSync, CheckpointedConnectorWithPermSync[BoxCheckpoint]
):
    def __init__(
        self,
        include_all_files: bool = False,
        folder_ids: str | None = None,
        exclude_domain_link_only: bool = False,
        batch_size: int = INDEX_BATCH_SIZE,
    ) -> None:
        if not include_all_files and not folder_ids:
            raise ConnectorValidationError(
                "Nothing to index. Please specify either 'include_all_files=True' "
                "or provide 'folder_ids' (comma-separated list of folder IDs or URLs)."
            )

        self.include_all_files = include_all_files
        folder_ids_str = folder_ids or ""
        folder_id_list = _extract_str_list_from_comma_str(folder_ids_str)
        # Also extract from URLs if provided
        folder_id_list.extend(_extract_ids_from_urls(folder_id_list))
        self._requested_folder_ids = set(folder_id_list)
        self.exclude_domain_link_only = exclude_domain_link_only

        self._box_client: BoxClient | None = None
        self._user_id: str | None = None
        self._creds_dict: dict[str, Any] | None = None

        # IDs of folders that have been traversed
        self._retrieved_folder_ids: set[str] = set()

        self.allow_images = False
        self.size_threshold = GOOGLE_DRIVE_CONNECTOR_SIZE_THRESHOLD

    def set_allow_images(self, value: bool) -> None:
        self.allow_images = value

    @property
    def box_client(self) -> BoxClient:
        if self._box_client is None:
            raise RuntimeError(
                "Box client missing, "
                "should not call this property "
                "before calling load_credentials"
            )
        return self._box_client

    @property
    def user_id(self) -> str:
        if self._user_id is None:
            raise RuntimeError(
                "User ID missing, "
                "should not call this property "
                "before calling load_credentials"
            )
        return self._user_id

    @classmethod
    @override
    def normalize_url(cls, url: str) -> NormalizationResult:
        """Normalize a Box URL to match the canonical Document.id format."""
        parsed = urlparse(url)
        netloc = parsed.netloc.lower()

        if not (netloc.startswith("app.box.com") or netloc.startswith("box.com")):
            return NormalizationResult(normalized_url=None, use_default=False)

        # Extract file/folder ID from path
        path_parts = parsed.path.strip("/").split("/")
        if len(path_parts) >= 2:
            item_id = path_parts[-1]
            # Construct normalized URL
            normalized = f"https://app.box.com/file/{item_id}"
            return NormalizationResult(normalized_url=normalized, use_default=False)

        return NormalizationResult(normalized_url=None, use_default=False)

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, str] | None:
        """Load Box credentials and initialize client."""
        # Support OAuth 2.0 tokens
        if "box_access_token" in credentials:
            access_token = credentials["box_access_token"]
            refresh_token = credentials.get("box_refresh_token")
            client_id = credentials.get("box_client_id")
            client_secret = credentials.get("box_client_secret")

            if refresh_token and client_id and client_secret:
                # Use OAuth with refresh token
                oauth = BoxOAuth(
                    client_id=client_id,
                    client_secret=client_secret,
                    access_token=access_token,
                    refresh_token=refresh_token,
                )
                self._box_client = BoxClient(oauth=oauth)
            else:
                # Use developer token (for testing)
                auth = BoxDeveloperTokenAuth(token=access_token)
                self._box_client = BoxClient(auth=auth)

            # Get current user info
            try:
                current_user = self._box_client.users.get_user_me()
                self._user_id = current_user.id
            except Exception as e:
                logger.warning(f"Could not get current user info: {e}")
                self._user_id = credentials.get("box_user_id", "unknown")

        elif "box_developer_token" in credentials:
            # Developer token authentication (for testing)
            auth = BoxDeveloperTokenAuth(token=credentials["box_developer_token"])
            self._box_client = BoxClient(auth=auth)
            self._user_id = credentials.get("box_user_id", "me")
        else:
            raise ConnectorValidationError(
                "Box credentials missing. Need either 'box_access_token' "
                "or 'box_developer_token'."
            )

        self._creds_dict = credentials
        return None

    def _update_traversed_folder_ids(self, folder_id: str) -> None:
        """Mark a folder as traversed."""
        self._retrieved_folder_ids.add(folder_id)

    def _fetch_box_items(
        self,
        checkpoint: BoxCheckpoint,
        start: SecondsSinceUnixEpoch | None = None,
        end: SecondsSinceUnixEpoch | None = None,
    ) -> Iterator[RetrievedBoxFile]:
        """Fetch Box files based on checkpoint state."""
        if checkpoint.completion_stage == BoxRetrievalStage.START:
            checkpoint.completion_stage = BoxRetrievalStage.FOLDER_FILES
            checkpoint.completion_map[self.user_id] = StageCompletion(
                stage=BoxRetrievalStage.START,
                completed_until=0,
                current_folder_id=None,
            )

        completion = checkpoint.completion_map.get(self.user_id)
        if not completion:
            completion = StageCompletion(
                stage=BoxRetrievalStage.START,
                completed_until=0,
                current_folder_id=None,
            )
            checkpoint.completion_map[self.user_id] = completion

        # Determine which folders to process
        if checkpoint.completion_stage == BoxRetrievalStage.FOLDER_FILES:
            if checkpoint.folder_ids_to_retrieve is None:
                if self.include_all_files:
                    # Start from root folder (ID "0")
                    checkpoint.folder_ids_to_retrieve = ["0"]
                else:
                    checkpoint.folder_ids_to_retrieve = sorted(
                        self._requested_folder_ids
                    )
            folder_ids = checkpoint.folder_ids_to_retrieve
        else:
            folder_ids = checkpoint.folder_ids_to_retrieve or []

        # Process folders
        for folder_id in folder_ids:
            if folder_id in self._retrieved_folder_ids:
                continue

            # Resume from checkpoint if needed
            if completion.current_folder_id == folder_id and completion.next_marker:
                # Resume from marker
                for file_or_marker in get_all_files_in_folder(
                    client=self.box_client,
                    folder_id=folder_id,
                    user_id=self.user_id,
                    start=(
                        completion.completed_until
                        if completion.completed_until > 0
                        else start
                    ),
                    end=end,
                    marker=completion.next_marker,
                ):
                    if isinstance(file_or_marker, str):
                        # This is a marker for next page
                        completion.next_marker = file_or_marker
                        return  # Checkpoint and resume later
                    yield file_or_marker
                    # Update completion timestamp
                    modified_time = file_or_marker.box_file.get("modified_at")
                    if modified_time:
                        try:
                            mod_dt = datetime.fromisoformat(
                                modified_time.replace("Z", "+00:00")
                            )
                            completion.completed_until = mod_dt.timestamp()
                        except (ValueError, AttributeError):
                            pass
            else:
                # Start fresh folder crawl
                completion.current_folder_id = folder_id
                completion.completed_until = 0
                completion.next_marker = None

                for file_or_marker in get_all_files_in_folder(
                    client=self.box_client,
                    folder_id=folder_id,
                    user_id=self.user_id,
                    start=start,
                    end=end,
                ):
                    if isinstance(file_or_marker, str):
                        # This is a marker for next page
                        completion.next_marker = file_or_marker
                        return  # Checkpoint and resume later
                    yield file_or_marker
                    # Update completion timestamp
                    modified_time = file_or_marker.box_file.get("modified_at")
                    if modified_time:
                        try:
                            mod_dt = datetime.fromisoformat(
                                modified_time.replace("Z", "+00:00")
                            )
                            completion.completed_until = mod_dt.timestamp()
                        except (ValueError, AttributeError):
                            pass

                # Also crawl subfolders recursively
                for retrieved_file in crawl_folders_for_files(
                    client=self.box_client,
                    parent_id=folder_id,
                    user_id=self.user_id,
                    traversed_parent_ids=self._retrieved_folder_ids,
                    update_traversed_ids_func=self._update_traversed_folder_ids,
                    start=start,
                    end=end,
                ):
                    yield retrieved_file

            # Mark folder as processed
            self._retrieved_folder_ids.add(folder_id)
            completion.current_folder_id = None
            completion.next_marker = None

        checkpoint.completion_stage = BoxRetrievalStage.DONE

    def _extract_docs_from_box(
        self,
        checkpoint: BoxCheckpoint,
        start: SecondsSinceUnixEpoch | None,
        end: SecondsSinceUnixEpoch | None,
        include_permissions: bool,
    ) -> Iterator[Document | ConnectorFailure]:
        """Retrieve and convert Box files to documents."""
        try:
            # Prepare conversion function
            permission_sync_context = (
                PermissionSyncContext(
                    primary_user_id=self.user_id,
                    box_domain=None,  # Box doesn't have domain concept like Google
                )
                if include_permissions
                else None
            )

            convert_func = partial(
                convert_box_item_to_document,
                self.box_client,
                self.allow_images,
                self.size_threshold,
                permission_sync_context,
                self.user_id,
            )

            # Fetch files
            for retrieved_file in self._fetch_box_items(
                checkpoint=checkpoint,
                start=start,
                end=end,
            ):
                if retrieved_file.error is not None:
                    failure_stage = retrieved_file.completion_stage.value
                    failure_message = (
                        f"retrieval failure during stage: {failure_stage}, "
                        f"user: {retrieved_file.user_id}, "
                        f"parent folder: {retrieved_file.parent_id}, "
                        f"error: {retrieved_file.error}"
                    )
                    logger.error(failure_message)
                    yield ConnectorFailure(
                        failed_entity=EntityFailure(entity_id=failure_stage),
                        failure_message=failure_message,
                        exception=retrieved_file.error,
                    )
                    continue

                box_file = retrieved_file.box_file
                if not box_file:
                    continue

                try:
                    document_id = onyx_document_id_from_box_file(box_file)
                except KeyError:
                    logger.warning(
                        f"Box file missing id (stage={retrieved_file.completion_stage} "
                        f"user={retrieved_file.user_id}). Skipping."
                    )
                    continue

                # Check for duplicates
                if document_id in checkpoint.all_retrieved_file_ids:
                    continue

                checkpoint.all_retrieved_file_ids.add(document_id)

                # Convert to document
                doc_or_failure = convert_func(box_file)
                if doc_or_failure:
                    yield doc_or_failure

            checkpoint.retrieved_folder_ids = self._retrieved_folder_ids

        except Exception as e:
            logger.exception(f"Error extracting documents from Box: {e}")
            raise e

    def _load_from_checkpoint(
        self,
        start: SecondsSinceUnixEpoch,
        end: SecondsSinceUnixEpoch,
        checkpoint: BoxCheckpoint,
        include_permissions: bool,
    ) -> CheckpointOutput[BoxCheckpoint]:
        """Entrypoint for the connector; first run is with an empty checkpoint."""
        if self._box_client is None or self._user_id is None:
            raise RuntimeError(
                "Credentials missing, should not call this method before calling load_credentials"
            )

        logger.info(
            f"Loading from checkpoint with completion stage: {checkpoint.completion_stage}, "
            f"num retrieved ids: {len(checkpoint.all_retrieved_file_ids)}"
        )
        checkpoint = copy.deepcopy(checkpoint)
        self._retrieved_folder_ids = checkpoint.retrieved_folder_ids

        try:
            yield from self._extract_docs_from_box(
                checkpoint, start, end, include_permissions
            )
        except Exception as e:
            raise e

        checkpoint.retrieved_folder_ids = self._retrieved_folder_ids

        logger.info(
            f"num box files retrieved: {len(checkpoint.all_retrieved_file_ids)}"
        )
        if checkpoint.completion_stage == BoxRetrievalStage.DONE:
            checkpoint.has_more = False
        return checkpoint

    @override
    def load_from_checkpoint(
        self,
        start: SecondsSinceUnixEpoch,
        end: SecondsSinceUnixEpoch,
        checkpoint: BoxCheckpoint,
    ) -> CheckpointOutput[BoxCheckpoint]:
        return self._load_from_checkpoint(
            start, end, checkpoint, include_permissions=False
        )

    @override
    def load_from_checkpoint_with_perm_sync(
        self,
        start: SecondsSinceUnixEpoch,
        end: SecondsSinceUnixEpoch,
        checkpoint: BoxCheckpoint,
    ) -> CheckpointOutput[BoxCheckpoint]:
        return self._load_from_checkpoint(
            start, end, checkpoint, include_permissions=True
        )

    def _extract_slim_docs_from_box(
        self,
        checkpoint: BoxCheckpoint,
        start: SecondsSinceUnixEpoch | None = None,
        end: SecondsSinceUnixEpoch | None = None,
        callback: IndexingHeartbeatInterface | None = None,
    ) -> GenerateSlimDocumentOutput:
        """Extract slim documents for permission syncing."""
        from onyx.connectors.google_utils.shared_constants import SLIM_BATCH_SIZE

        slim_batch = []
        for file in self._fetch_box_items(
            checkpoint=checkpoint,
            start=start,
            end=end,
        ):
            if file.error is not None:
                raise file.error

            if doc := build_slim_document(
                self.box_client,
                file.box_file,
                PermissionSyncContext(
                    primary_user_id=self.user_id,
                    box_domain=None,
                ),
            ):
                slim_batch.append(doc)
            if len(slim_batch) >= SLIM_BATCH_SIZE:
                yield slim_batch
                slim_batch = []
                if callback:
                    if callback.should_stop():
                        raise RuntimeError(
                            "_extract_slim_docs_from_box: Stop signal detected"
                        )
                    callback.progress("_extract_slim_docs_from_box", 1)
        yield slim_batch

    def retrieve_all_slim_docs_perm_sync(
        self,
        start: SecondsSinceUnixEpoch | None = None,
        end: SecondsSinceUnixEpoch | None = None,
        callback: IndexingHeartbeatInterface | None = None,
    ) -> GenerateSlimDocumentOutput:
        """Retrieve all slim documents for permission syncing."""
        try:
            checkpoint = self.build_dummy_checkpoint()
            while checkpoint.completion_stage != BoxRetrievalStage.DONE:
                yield from self._extract_slim_docs_from_box(
                    checkpoint=checkpoint,
                    start=start,
                    end=end,
                    callback=callback,
                )
            logger.info("Box perm sync: Slim doc retrieval complete")

        except Exception as e:
            raise e

    def validate_connector_settings(self) -> None:
        """Validate Box connector settings and credentials."""
        if self._box_client is None:
            raise ConnectorMissingCredentialError("Box credentials not loaded.")

        try:
            # Test API access by getting current user
            current_user = self._box_client.users.get_user_me()
            logger.info(f"Box connector validated for user: {current_user.name}")

        except BoxAPIException as e:
            status_code = e.status_code if hasattr(e, "status_code") else None
            if status_code == 401:
                raise CredentialExpiredError(
                    "Invalid or expired Box credentials (401)."
                )
            elif status_code == 403:
                raise InsufficientPermissionsError(
                    "Box app lacks required permissions (403). "
                    "Please ensure the necessary scopes are granted."
                )
            else:
                raise ConnectorValidationError(
                    f"Unexpected Box error (status={status_code}): {e}"
                )
        except Exception as e:
            raise ConnectorValidationError(
                f"Unexpected error during Box validation: {e}"
            )

    @override
    def build_dummy_checkpoint(self) -> BoxCheckpoint:
        """Build an initial empty checkpoint."""
        return BoxCheckpoint(
            retrieved_folder_ids=set(),
            completion_stage=BoxRetrievalStage.START,
            completion_map=ThreadSafeDict(),
            all_retrieved_file_ids=set(),
            has_more=True,
        )

    @override
    def validate_checkpoint_json(self, checkpoint_json: str) -> BoxCheckpoint:
        """Validate checkpoint JSON and return checkpoint object."""
        return BoxCheckpoint.model_validate_json(checkpoint_json)
