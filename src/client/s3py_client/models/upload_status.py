from enum import Enum


class UploadStatus(str, Enum):
    COMPLETED = "completed"
    INITIATED = "initiated"
    IN_PROGRESS = "in-progress"

    def __str__(self) -> str:
        return str(self.value)
