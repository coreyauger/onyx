from collections.abc import Callable
from collections.abc import Iterator
from datetime import datetime
from datetime import timezone
from enum import Enum

from box_sdk_gen.client import BoxClient
from box_sdk_gen.schemas import File as BoxFile
from box_sdk_gen.schemas import Folder as BoxFolder

from onyx.connectors.box.models import BoxFileType
from onyx.connectors.box.models import BoxRetrievalStage
from onyx.connectors.box.models import RetrievedBoxFile
from onyx.connectors.interfaces import SecondsSinceUnixEpoch
from onyx.utils.logger import setup_logger

logger = setup_logger()


class BoxFileFieldType(Enum):
    """Enum to specify which fields to retrieve from Box files"""

    SLIM = "slim"  # Minimal fields for basic file info
    STANDARD = "standard"  # Standard fields including content metadata
    WITH_PERMISSIONS = "with_permissions"  # Full fields including permissions


def _box_file_to_dict(file: BoxFile | BoxFolder) -> BoxFileType:
    """Convert Box SDK file/folder object to dictionary."""
    return {
        "id": file.id,
        "name": file.name,
        "type": file.type.value if hasattr(file.type, "value") else str(file.type),
        "modified_at": file.modified_at.isoformat() if file.modified_at else None,
        "created_at": file.created_at.isoformat() if file.created_at else None,
        "size": file.size if hasattr(file, "size") and file.size is not None else 0,
        "parent": (
            {"id": file.parent.id} if hasattr(file, "parent") and file.parent else None
        ),
        "shared_link": (
            file.shared_link.url
            if hasattr(file, "shared_link") and file.shared_link
            else None
        ),
        "permissions": (
            _extract_permissions(file)
            if hasattr(file, "permissions") and file.permissions
            else []
        ),
    }


def _extract_permissions(file: BoxFile | BoxFolder) -> list[dict[str, any]]:
    """Extract permissions from Box file/folder object."""
    if not hasattr(file, "permissions") or not file.permissions:
        return []
    # Box permissions structure is different, adapt as needed
    return []


def generate_time_range_filter(
    start: SecondsSinceUnixEpoch | None = None,
    end: SecondsSinceUnixEpoch | None = None,
) -> dict[str, str]:
    """Generate time range filter for Box API."""
    filters: dict[str, str] = {}
    if start is not None:
        time_start = datetime.fromtimestamp(start, tz=timezone.utc).isoformat()
        filters["created_at_range"] = f"{time_start},"
    if end is not None:
        time_stop = datetime.fromtimestamp(end, tz=timezone.utc).isoformat()
        if "created_at_range" in filters:
            filters["created_at_range"] += time_stop
        else:
            filters["created_at_range"] = f",{time_stop}"
    return filters


def _get_folders_in_parent(
    client: BoxClient,
    parent_id: str = "0",  # "0" is root folder in Box
) -> Iterator[BoxFileType]:
    """Get all folders in a parent folder."""
    try:
        items = client.folders.get_folder_items(
            folder_id=parent_id,
            fields=["id", "name", "type", "modified_at", "created_at", "parent"],
        )
        for item in items.entries:
            if item.type.value == "folder":
                yield _box_file_to_dict(item)
    except Exception as e:
        logger.warning(f"Error getting folders in parent {parent_id}: {e}")
        # Continue on error, similar to Google Drive behavior


def _get_files_in_parent(
    client: BoxClient,
    parent_id: str = "0",
    start: SecondsSinceUnixEpoch | None = None,
    end: SecondsSinceUnixEpoch | None = None,
) -> Iterator[BoxFileType]:
    """Get all files in a parent folder."""
    try:
        # Box API uses limit and marker for pagination
        limit = 1000  # Max items per page in Box
        marker: str | None = None

        while True:
            items = client.folders.get_folder_items(
                folder_id=parent_id,
                fields=[
                    "id",
                    "name",
                    "type",
                    "modified_at",
                    "created_at",
                    "size",
                    "parent",
                    "shared_link",
                ],
                limit=limit,
                marker=marker,
            )

            for item in items.entries:
                if item.type.value == "file":
                    file_dict = _box_file_to_dict(item)
                    # Apply time filter if needed
                    if start or end:
                        modified_time = file_dict.get("modified_at")
                        if modified_time:
                            try:
                                mod_dt = datetime.fromisoformat(
                                    modified_time.replace("Z", "+00:00")
                                )
                                mod_ts = mod_dt.timestamp()
                                if start and mod_ts < start:
                                    continue
                                if end and mod_ts > end:
                                    continue
                            except (ValueError, AttributeError):
                                pass
                    yield file_dict

            # Check if there are more pages
            if items.entries and len(items.entries) == limit:
                # Box uses the last item's ID as marker for next page
                marker = items.entries[-1].id
            else:
                break

    except Exception as e:
        logger.warning(f"Error getting files in parent {parent_id}: {e}")


def crawl_folders_for_files(
    client: BoxClient,
    parent_id: str,
    user_id: str,
    traversed_parent_ids: set[str],
    update_traversed_ids_func: Callable[[str], None],
    start: SecondsSinceUnixEpoch | None = None,
    end: SecondsSinceUnixEpoch | None = None,
) -> Iterator[RetrievedBoxFile]:
    """
    Recursively crawl folders to get all files.
    This function starts crawling from any folder.
    """
    logger.info(f"Entered crawl_folders_for_files with parent_id: {parent_id}")
    if parent_id not in traversed_parent_ids:
        logger.info("Parent id not in traversed parent ids, getting files")
        found_files = False
        try:
            for file_dict in _get_files_in_parent(
                client=client,
                parent_id=parent_id,
                start=start,
                end=end,
            ):
                logger.info(f"Found file: {file_dict.get('name')}, user id: {user_id}")
                found_files = True
                yield RetrievedBoxFile(
                    box_file=file_dict,
                    user_id=user_id,
                    parent_id=parent_id,
                    completion_stage=BoxRetrievalStage.FOLDER_FILES,
                )
            # Only mark a folder as done if it was fully traversed without errors
            if found_files:
                update_traversed_ids_func(parent_id)
        except Exception as e:
            logger.error(f"Error getting files in parent {parent_id}: {e}")
            yield RetrievedBoxFile(
                box_file={},
                user_id=user_id,
                parent_id=parent_id,
                completion_stage=BoxRetrievalStage.FOLDER_FILES,
                error=e,
            )
    else:
        logger.info(f"Skipping subfolder files since already traversed: {parent_id}")

    # Recursively process subfolders
    for folder_dict in _get_folders_in_parent(client=client, parent_id=parent_id):
        folder_id = folder_dict.get("id")
        if folder_id:
            logger.info(f"Fetching all files in subfolder: {folder_dict.get('name')}")
            yield from crawl_folders_for_files(
                client=client,
                parent_id=folder_id,
                user_id=user_id,
                traversed_parent_ids=traversed_parent_ids,
                update_traversed_ids_func=update_traversed_ids_func,
                start=start,
                end=end,
            )


def get_all_files_in_folder(
    client: BoxClient,
    folder_id: str = "0",
    user_id: str = "me",
    start: SecondsSinceUnixEpoch | None = None,
    end: SecondsSinceUnixEpoch | None = None,
    marker: str | None = None,
) -> Iterator[RetrievedBoxFile | str]:
    """
    Get all files in a folder (non-recursive).
    Returns RetrievedBoxFile objects or a marker string for pagination.
    """
    try:
        limit = 1000
        current_marker = marker

        while True:
            items = client.folders.get_folder_items(
                folder_id=folder_id,
                fields=[
                    "id",
                    "name",
                    "type",
                    "modified_at",
                    "created_at",
                    "size",
                    "parent",
                    "shared_link",
                ],
                limit=limit,
                marker=current_marker,
            )

            for item in items.entries:
                if item.type.value == "file":
                    file_dict = _box_file_to_dict(item)
                    # Apply time filter
                    if start or end:
                        modified_time = file_dict.get("modified_at")
                        if modified_time:
                            try:
                                mod_dt = datetime.fromisoformat(
                                    modified_time.replace("Z", "+00:00")
                                )
                                mod_ts = mod_dt.timestamp()
                                if start and mod_ts < start:
                                    continue
                                if end and mod_ts > end:
                                    continue
                            except (ValueError, AttributeError):
                                pass

                    yield RetrievedBoxFile(
                        box_file=file_dict,
                        user_id=user_id,
                        parent_id=folder_id,
                        completion_stage=BoxRetrievalStage.FOLDER_FILES,
                    )

            # Check for more pages
            if items.entries and len(items.entries) == limit:
                current_marker = items.entries[-1].id
                yield current_marker  # Return marker for checkpoint
                break
            else:
                break

    except Exception as e:
        logger.error(f"Error getting all files in folder {folder_id}: {e}")
        yield RetrievedBoxFile(
            box_file={},
            user_id=user_id,
            parent_id=folder_id,
            completion_stage=BoxRetrievalStage.FOLDER_FILES,
            error=e,
        )
