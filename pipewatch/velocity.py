"""Velocity tracking for pipeline runs.

Measures throughput (runs per hour/day) and detects significant
changes in run frequency that may indicate upstream issues.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from pipewatch.models import PipelineRun


@dataclass
class VelocityResult:
    """Velocity metrics for a single pipeline."""

    pipeline: str
    window_hours: int
    run_count: int
    runs_per_hour: float
    prev_runs_per_hour: float  # same-length window immediately before
    pct_change: Optional[float]  # None when no previous data
    is_accelerating: bool
    is_stalling: bool  # run rate dropped > stall_threshold %

    def __str__(self) -> str:
        direction = ""
        if self.pct_change is not None:
            sign = "+" if self.pct_change >= 0 else ""
            direction = f"  ({sign}{self.pct_change:.1f}% vs prev window)"
        stall_flag = "  ⚠ STALLING" if self.is_stalling else ""
        accel_flag = "  ↑ ACCELERATING" if self.is_accelerating else ""
        return (
            f"{self.pipeline}: {self.runs_per_hour:.2f} runs/hr "
            f"over {self.window_hours}h ({self.run_count} runs)"
            f"{direction}{stall_flag}{accel_flag}"
        )


def _runs_in_window(
    runs: List[PipelineRun],
    start: datetime,
    end: datetime,
) -> List[PipelineRun]:
    """Return runs whose started_at falls within [start, end)."""
    result = []
    for r in runs:
        ts = r.started_at
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if start <= ts < end:
            result.append(r)
    return result


def compute_velocity(
    runs: List[PipelineRun],
    window_hours: int = 24,
    stall_threshold: float = 50.0,
    accel_threshold: float = 50.0,
    pipeline: Optional[str] = None,
    now: Optional[datetime] = None,
) -> List[VelocityResult]:
    """Compute run velocity for each pipeline.

    Args:
        runs: All pipeline runs to analyse.
        window_hours: Length of the current observation window in hours.
        stall_threshold: % drop in runs/hr that marks a pipeline as stalling.
        accel_threshold: % increase in runs/hr that marks acceleration.
        pipeline: If set, restrict output to this pipeline only.
        now: Reference timestamp (defaults to UTC now).

    Returns:
        One VelocityResult per pipeline, sorted by pipeline name.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    window = timedelta(hours=window_hours)
    current_start = now - window
    prev_start = current_start - window

    # Group runs by pipeline
    by_pipeline: Dict[str, List[PipelineRun]] = {}
    for run in runs:
        if pipeline and run.pipeline != pipeline:
            continue
        by_pipeline.setdefault(run.pipeline, []).append(run)

    results: List[VelocityResult] = []
    for pipe_name, pipe_runs in sorted(by_pipeline.items()):
        current_runs = _runs_in_window(pipe_runs, current_start, now)
        prev_runs = _runs_in_window(pipe_runs, prev_start, current_start)

        runs_per_hour = len(current_runs) / window_hours
        prev_runs_per_hour = len(prev_runs) / window_hours

        if prev_runs_per_hour > 0:
            pct_change = (
                (runs_per_hour - prev_runs_per_hour) / prev_runs_per_hour * 100
            )
        elif len(prev_runs) == 0 and len(current_runs) > 0:
            pct_change = None  # no baseline to compare against
        else:
            pct_change = None

        is_stalling = (
            pct_change is not None and pct_change <= -stall_threshold
        )
        is_accelerating = (
            pct_change is not None and pct_change >= accel_threshold
        )

        results.append(
            VelocityResult(
                pipeline=pipe_name,
                window_hours=window_hours,
                run_count=len(current_runs),
                runs_per_hour=runs_per_hour,
                prev_runs_per_hour=prev_runs_per_hour,
                pct_change=pct_change,
                is_accelerating=is_accelerating,
                is_stalling=is_stalling,
            )
        )

    return results
