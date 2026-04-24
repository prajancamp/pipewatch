"""Sliding window aggregation over pipeline runs."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class WindowSummary:
    pipeline: str
    window_minutes: int
    total: int
    failures: int
    successes: int
    avg_duration: Optional[float]

    @property
    def failure_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.failures / self.total

    def __str__(self) -> str:
        rate = f"{self.failure_rate * 100:.1f}%"
        dur = f"{self.avg_duration:.1f}s" if self.avg_duration is not None else "n/a"
        return (
            f"{self.pipeline} | window={self.window_minutes}m "
            f"total={self.total} failures={self.failures} "
            f"failure_rate={rate} avg_duration={dur}"
        )


def _now() -> datetime:
    return datetime.utcnow()


def compute_window(
    runs: List[PipelineRun],
    window_minutes: int = 60,
    pipeline: Optional[str] = None,
    reference_time: Optional[datetime] = None,
) -> List[WindowSummary]:
    """Compute sliding window summaries for each pipeline.

    Args:
        runs: All pipeline runs to consider.
        window_minutes: Width of the sliding window in minutes.
        pipeline: If set, restrict to a single pipeline.
        reference_time: Treat this as 'now'; defaults to UTC now.

    Returns:
        A list of WindowSummary, one per pipeline found in the window.
    """
    now = reference_time or _now()
    cutoff = now - timedelta(minutes=window_minutes)

    filtered = [
        r for r in runs
        if r.started_at is not None and r.started_at >= cutoff
    ]
    if pipeline:
        filtered = [r for r in filtered if r.pipeline == pipeline]

    pipelines: dict[str, List[PipelineRun]] = {}
    for run in filtered:
        pipelines.setdefault(run.pipeline, []).append(run)

    summaries: List[WindowSummary] = []
    for pipe, pipe_runs in sorted(pipelines.items()):
        failures = sum(1 for r in pipe_runs if r.is_failed())
        successes = sum(1 for r in pipe_runs if r.is_success())
        durations = [r.duration for r in pipe_runs if r.duration is not None]
        avg_dur = sum(durations) / len(durations) if durations else None
        summaries.append(
            WindowSummary(
                pipeline=pipe,
                window_minutes=window_minutes,
                total=len(pipe_runs),
                failures=failures,
                successes=successes,
                avg_duration=avg_dur,
            )
        )
    return summaries
