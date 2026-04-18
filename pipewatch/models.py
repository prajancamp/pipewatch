from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class PipelineStatus(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    RUNNING = "running"
    SKIPPED = "skipped"


@dataclass
class PipelineRun:
    pipeline_id: str
    run_id: str
    status: PipelineStatus
    started_at: datetime
    finished_at: Optional[datetime] = None
    error_message: Optional[str] = None
    duration_seconds: Optional[float] = None
    tags: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.finished_at and self.started_at:
            self.duration_seconds = (
                self.finished_at - self.started_at
            ).total_seconds()

    @property
    def is_failed(self) -> bool:
        return self.status == PipelineStatus.FAILURE

    @property
    def is_success(self) -> bool:
        return self.status == PipelineStatus.SUCCESS

    def to_dict(self) -> dict:
        return {
            "pipeline_id": self.pipeline_id,
            "run_id": self.run_id,
            "status": self.status.value,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "error_message": self.error_message,
            "duration_seconds": self.duration_seconds,
            "tags": self.tags,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PipelineRun":
        return cls(
            pipeline_id=data["pipeline_id"],
            run_id=data["run_id"],
            status=PipelineStatus(data["status"]),
            started_at=datetime.fromisoformat(data["started_at"]),
            finished_at=datetime.fromisoformat(data["finished_at"]) if data.get("finished_at") else None,
            error_message=data.get("error_message"),
            tags=data.get("tags", {}),
        )
