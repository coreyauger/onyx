from enum import Enum
from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_serializer
from pydantic import field_validator

from onyx.connectors.interfaces import ConnectorCheckpoint
from onyx.connectors.interfaces import SecondsSinceUnixEpoch
from onyx.utils.threadpool_concurrency import ThreadSafeDict


BoxFileType = dict[str, Any]


class BoxRetrievalStage(str, Enum):
    """Stages of retrieval for Box connector."""

    START = "start"
    FOLDER_FILES = "folder_files"
    DONE = "done"


class StageCompletion(BaseModel):
    """
    Describes the point in the retrieval+indexing process that the
    connector is at. completed_until is the timestamp of the latest
    file that has been retrieved or error that has been yielded.
    Optional fields are used for retrieval stages that need more information
    for resuming than just the timestamp of the latest file.
    """

    stage: BoxRetrievalStage
    completed_until: SecondsSinceUnixEpoch
    current_folder_id: str | None = None
    next_marker: str | None = None

    # Track which folders have been processed
    processed_folder_ids: set[str] = set()

    def update(
        self,
        stage: BoxRetrievalStage,
        completed_until: SecondsSinceUnixEpoch,
        current_folder_id: str | None = None,
    ) -> None:
        self.stage = stage
        self.completed_until = completed_until
        self.current_folder_id = current_folder_id


class RetrievedBoxFile(BaseModel):
    """
    Describes a file that has been retrieved from Box.
    user_id is the ID of the user that the file was retrieved by.
    If an error worthy of being reported is encountered,
    error should be set and later propagated as a ConnectorFailure.
    """

    # The stage at which this file was retrieved
    completion_stage: BoxRetrievalStage

    # The file that was retrieved
    box_file: BoxFileType

    # The ID of the user that the file was retrieved by
    user_id: str

    # The id of the parent folder of the file
    parent_id: str | None = None

    # Any unexpected error that occurred while retrieving the file.
    error: Exception | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)


class BoxCheckpoint(ConnectorCheckpoint):
    """Checkpoint for Box connector retrieval state."""

    # Checkpoint version of _retrieved_ids
    retrieved_folder_ids: set[str]

    # Describes the point in the retrieval+indexing process that the
    # checkpoint is at. when this is set to a given stage, the connector
    # has finished yielding all values from the previous stage.
    completion_stage: BoxRetrievalStage

    # The latest timestamp of a file that has been retrieved per user ID.
    completion_map: ThreadSafeDict[str, StageCompletion]

    # all file ids that have been retrieved
    all_retrieved_file_ids: set[str] = set()

    # cached version of the folder ids to retrieve
    folder_ids_to_retrieve: list[str] | None = None

    @field_serializer("completion_map")
    def serialize_completion_map(
        self, completion_map: ThreadSafeDict[str, StageCompletion], _info: Any
    ) -> dict[str, StageCompletion]:
        return completion_map._dict

    @field_validator("completion_map", mode="before")
    def validate_completion_map(cls, v: Any) -> ThreadSafeDict[str, StageCompletion]:
        assert isinstance(v, dict) or isinstance(v, ThreadSafeDict)
        return ThreadSafeDict(
            {k: StageCompletion.model_validate(val) for k, val in v.items()}
        )
