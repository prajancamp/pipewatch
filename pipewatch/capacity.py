"""Capacity planning module: estimates future run volume and resource needs."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class CapacityResult:
    pipeline: str
    window_hours: int
    run_count: int
    avg_duration_seconds: Optional[float]
    projected_runs_per_day: float
    projected_compute_minutes_per_day: float
    note: str

    def __str__(self) -> str:
        dur = (
            f"{self.avg_duration_seconds:.1f}s avg duration"
            if self.avg_duration_seconds is not None
            else "no duration data"
        )
        return (
            f"[{self.pipeline}] {self.projected_runs_per_day:.1f} runs/day projected "
            f"({dur}, {self.projected_compute_minutes_per_day:.1f} compute-min/day) — {self.note}"
        )


def _runs_in_window(runs: List[PipelineRun], pipeline: str, window_hours: int) -> List[PipelineRun]:
    from datetime import datetime, timezone, timedelta

    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    return [
        r for r in runs
        if r.pipeline == pipeline and r.started_at >= cutoff
    ]


def estimate_capacity(
    runs: List[PipelineRun],
    pipeline: str,
    window_hours: int = 24,
) -> Optional[CapacityResult]:
    """Estimate capacity needs for a single pipeline based on recent history."""
    recent = _runs_in_window(runs, pipeline, window_hours)
    if not recent:
        return None

    run_count = len(recent)
    hours_elapsed = window_hours or 1
    projected_runs_per_day = (run_count / hours_elapsed) * 24

    durations = [r.duration_seconds for r in recent if r.duration_seconds is not None]
    avg_duration = sum(durations) / len(durations) if durations else None

    if avg_duration is not None:
        projected_compute_minutes = projected_runs_per_day * avg_duration / 60
    else:
        projected_compute_minutes = 0.0

    if projected_runs_per_day > 100:
        note = "HIGH volume — consider parallelisation"
    elif projected_runs_per_day > 20:
        note = "MODERATE volume"
    else:
        note = "LOW volume"

    return CapacityResult(
        pipeline=pipeline,
        window_hours=window_hours,
        run_count=run_count,
        avg_duration_seconds=avg_duration,
        projected_runs_per_day=projected_runs_per_day,
        projected_compute_minutes_per_day=projected_compute_minutes,
        note=note,
    )


def estimate_all_capacity(
    runs: List[PipelineRun],
    window_hours: int = 24,
) -> List[CapacityResult]:
    """Estimate capacity for every pipeline present in *runs*."""
    pipelines = sorted({r.pipeline for r in runs})
    results = []
    for name in pipelines:
        result = estimate_capacity(runs, name, window_hours)
        if result is not None:
            results.append(result)
    return results
