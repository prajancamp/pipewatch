"""Checkpoint tracking: record and query the last successful run per pipeline."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from pipewatch.models import PipelineRun


def _checkpoint_path(store_path: str) -> Path:
    return Path(store_path).parent / "checkpoints.json"


@dataclass
class CheckpointEntry:
    pipeline: str
    last_success: str          # ISO timestamp
    run_id: str
    duration: Optional[float]

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "last_success": self.last_success,
            "run_id": self.run_id,
            "duration": self.duration,
        }

    @staticmethod
    def from_dict(d: dict) -> "CheckpointEntry":
        return CheckpointEntry(
            pipeline=d["pipeline"],
            last_success=d["last_success"],
            run_id=d["run_id"],
            duration=d.get("duration"),
        )

    def __str__(self) -> str:
        dur = f"{self.duration:.1f}s" if self.duration is not None else "n/a"
        return f"{self.pipeline}: last success at {self.last_success} (run={self.run_id}, dur={dur})"


def load_checkpoints(store_path: str) -> Dict[str, CheckpointEntry]:
    path = _checkpoint_path(store_path)
    if not path.exists():
        return {}
    data = json.loads(path.read_text())
    return {k: CheckpointEntry.from_dict(v) for k, v in data.items()}


def save_checkpoints(store_path: str, checkpoints: Dict[str, CheckpointEntry]) -> None:
    path = _checkpoint_path(store_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({k: v.to_dict() for k, v in checkpoints.items()}, indent=2))


def update_checkpoints(store_path: str, runs: List[PipelineRun]) -> Dict[str, CheckpointEntry]:
    """Update checkpoint entries from a list of runs; only successful runs advance the checkpoint."""
    checkpoints = load_checkpoints(store_path)
    for run in runs:
        if not run.is_success():
            continue
        existing = checkpoints.get(run.pipeline)
        if existing is None or run.started_at > existing.last_success:
            checkpoints[run.pipeline] = CheckpointEntry(
                pipeline=run.pipeline,
                last_success=run.started_at,
                run_id=run.run_id,
                duration=run.duration,
            )
    save_checkpoints(store_path, checkpoints)
    return checkpoints


def get_checkpoint(store_path: str, pipeline: str) -> Optional[CheckpointEntry]:
    return load_checkpoints(store_path).get(pipeline)


def seconds_since_checkpoint(entry: CheckpointEntry) -> float:
    last = datetime.fromisoformat(entry.last_success)
    return (datetime.utcnow() - last).total_seconds()
