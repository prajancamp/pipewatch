"""Detect pipelines that have stopped running (stalled) within an expected cadence."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class StallResult:
    pipeline: str
    last_run_at: Optional[datetime]
    minutes_since_last_run: Optional[float]
    expected_interval_minutes: float
    is_stalled: bool
    message: str

    def __str__(self) -> str:
        status = "STALLED" if self.is_stalled else "OK"
        if self.last_run_at is None:
            return f"[{status}] {self.pipeline}: never run (expected every {self.expected_interval_minutes:.0f}m)"
        return (
            f"[{status}] {self.pipeline}: last run {self.minutes_since_last_run:.1f}m ago "
            f"(expected every {self.expected_interval_minutes:.0f}m)"
        )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def detect_stalls(
    runs: List[PipelineRun],
    expected_interval_minutes: float,
    pipeline: Optional[str] = None,
    now: Optional[datetime] = None,
) -> List[StallResult]:
    """Return a StallResult per pipeline indicating whether it has stalled.

    A pipeline is considered stalled if its most recent run started more than
    ``expected_interval_minutes`` ago (or has never run).
    """
    if now is None:
        now = _now()

    # Group latest run per pipeline
    latest: dict[str, PipelineRun] = {}
    for run in runs:
        if pipeline and run.pipeline != pipeline:
            continue
        prev = latest.get(run.pipeline)
        if prev is None or run.started_at > prev.started_at:
            latest[run.pipeline] = run

    results: List[StallResult] = []
    for pipe_name, last_run in sorted(latest.items()):
        started = last_run.started_at
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        delta_minutes = (now - started).total_seconds() / 60.0
        stalled = delta_minutes > expected_interval_minutes
        msg = (
            f"No run in the last {expected_interval_minutes:.0f} minutes."
            if stalled
            else "Running on schedule."
        )
        results.append(
            StallResult(
                pipeline=pipe_name,
                last_run_at=started,
                minutes_since_last_run=round(delta_minutes, 2),
                expected_interval_minutes=expected_interval_minutes,
                is_stalled=stalled,
                message=msg,
            )
        )

    return results
