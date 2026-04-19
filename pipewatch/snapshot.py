"""Snapshot: capture and compare pipeline state at a point in time."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from pipewatch.analyzer import PipelineStats
from pipewatch.summary import stats_to_dict


@dataclass
class Snapshot:
    captured_at: str
    pipelines: Dict[str, dict] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"captured_at": self.captured_at, "pipelines": self.pipelines}

    @classmethod
    def from_dict(cls, data: dict) -> "Snapshot":
        return cls(captured_at=data["captured_at"], pipelines=data.get("pipelines", {}))


def capture_snapshot(stats_list: List[PipelineStats]) -> Snapshot:
    """Build a Snapshot from a list of PipelineStats."""
    now = datetime.now(timezone.utc).isoformat()
    pipelines = {s.pipeline_id: stats_to_dict(s) for s in stats_list}
    return Snapshot(captured_at=now, pipelines=pipelines)


def save_snapshot(snapshot: Snapshot, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot.to_dict(), indent=2))


def load_snapshot(path: Path) -> Optional[Snapshot]:
    if not path.exists():
        return None
    return Snapshot.from_dict(json.loads(path.read_text()))


def diff_snapshots(old: Snapshot, new: Snapshot) -> Dict[str, dict]:
    """Return per-pipeline diff of success_rate and avg_duration."""
    result = {}
    for pid, new_stats in new.pipelines.items():
        old_stats = old.pipelines.get(pid)
        if old_stats is None:
            result[pid] = {"new": True}
            continue
        delta = {}
        for key in ("success_rate", "avg_duration"):
            ov = old_stats.get(key)
            nv = new_stats.get(key)
            if ov is not None and nv is not None:
                delta[key] = round(nv - ov, 4)
        if delta:
            result[pid] = delta
    return result
