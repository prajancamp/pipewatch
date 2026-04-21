"""Momentum analysis: measures whether a pipeline's run frequency is accelerating or decelerating."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class MomentumResult:
    pipeline: str
    recent_run_count: int
    prior_run_count: int
    recent_window_hours: float
    prior_window_hours: float
    delta: int  # recent_run_count - prior_run_count
    trend: str  # "accelerating", "decelerating", "stable", "insufficient_data"

    def __str__(self) -> str:
        arrow = {"accelerating": "↑", "decelerating": "↓", "stable": "→"}.get(self.trend, "?")
        return (
            f"{self.pipeline}: {arrow} {self.trend} "
            f"(recent={self.recent_run_count}, prior={self.prior_run_count}, Δ={self.delta:+d})"
        )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _runs_in_range(runs: List[PipelineRun], start: datetime, end: datetime) -> List[PipelineRun]:
    result = []
    for r in runs:
        ts = r.started_at
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if start <= ts < end:
            result.append(r)
    return result


def compute_momentum(
    runs: List[PipelineRun],
    pipeline: str,
    window_hours: float = 24.0,
    min_runs: int = 2,
    now: Optional[datetime] = None,
) -> MomentumResult:
    """Compare run frequency in the most recent window vs the prior equal window."""
    if now is None:
        now = _now()
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    from datetime import timedelta

    pipeline_runs = [r for r in runs if r.pipeline == pipeline]

    window = timedelta(hours=window_hours)
    recent_start = now - window
    prior_start = now - 2 * window

    recent = _runs_in_range(pipeline_runs, recent_start, now)
    prior = _runs_in_range(pipeline_runs, prior_start, recent_start)

    total = len(recent) + len(prior)
    if total < min_runs:
        return MomentumResult(
            pipeline=pipeline,
            recent_run_count=len(recent),
            prior_run_count=len(prior),
            recent_window_hours=window_hours,
            prior_window_hours=window_hours,
            delta=len(recent) - len(prior),
            trend="insufficient_data",
        )

    delta = len(recent) - len(prior)
    if delta > 0:
        trend = "accelerating"
    elif delta < 0:
        trend = "decelerating"
    else:
        trend = "stable"

    return MomentumResult(
        pipeline=pipeline,
        recent_run_count=len(recent),
        prior_run_count=len(prior),
        recent_window_hours=window_hours,
        prior_window_hours=window_hours,
        delta=delta,
        trend=trend,
    )


def compute_all_momentums(
    runs: List[PipelineRun],
    window_hours: float = 24.0,
    min_runs: int = 2,
    now: Optional[datetime] = None,
) -> List[MomentumResult]:
    """Compute momentum for every distinct pipeline in runs."""
    pipelines = sorted({r.pipeline for r in runs})
    return [
        compute_momentum(runs, p, window_hours=window_hours, min_runs=min_runs, now=now)
        for p in pipelines
    ]
