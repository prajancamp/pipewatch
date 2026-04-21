"""Pulse: periodic heartbeat tracking for pipeline activity."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.models import PipelineRun


@dataclass
class PulseResult:
    pipeline: str
    last_seen: Optional[datetime]
    runs_last_hour: int
    runs_last_day: int
    is_active: bool  # at least one run in the last day

    def __str__(self) -> str:
        status = "ACTIVE" if self.is_active else "SILENT"
        last = self.last_seen.strftime("%Y-%m-%d %H:%M:%S") if self.last_seen else "never"
        return (
            f"[{status}] {self.pipeline}  "
            f"last={last}  1h={self.runs_last_hour}  24h={self.runs_last_day}"
        )


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def compute_pulse(runs: List[PipelineRun], pipeline: str) -> PulseResult:
    """Compute heartbeat stats for a single pipeline."""
    now = _now()
    pipeline_runs = [r for r in runs if r.pipeline == pipeline]
    pipeline_runs.sort(key=lambda r: r.started_at)

    last_seen: Optional[datetime] = pipeline_runs[-1].started_at if pipeline_runs else None

    one_hour_ago = now.timestamp() - 3600
    one_day_ago = now.timestamp() - 86400

    runs_last_hour = sum(
        1 for r in pipeline_runs if r.started_at.timestamp() >= one_hour_ago
    )
    runs_last_day = sum(
        1 for r in pipeline_runs if r.started_at.timestamp() >= one_day_ago
    )

    is_active = runs_last_day > 0
    return PulseResult(
        pipeline=pipeline,
        last_seen=last_seen,
        runs_last_hour=runs_last_hour,
        runs_last_day=runs_last_day,
        is_active=is_active,
    )


def compute_all_pulses(runs: List[PipelineRun]) -> Dict[str, PulseResult]:
    """Compute pulse for every pipeline present in runs."""
    pipelines = sorted({r.pipeline for r in runs})
    return {p: compute_pulse(runs, p) for p in pipelines}


def silent_pipelines(pulses: Dict[str, PulseResult]) -> List[PulseResult]:
    """Return pipelines with no activity in the last 24 hours."""
    return [p for p in pulses.values() if not p.is_active]
