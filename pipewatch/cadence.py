"""Cadence analysis: detect pipelines running ahead/behind their expected schedule."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import mean, stdev
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class CadenceResult:
    pipeline: str
    expected_interval_minutes: Optional[float]  # inferred from history
    actual_last_gap_minutes: Optional[float]    # gap since most recent run
    run_count: int
    status: str  # "on_time", "overdue", "too_frequent", "insufficient_data"
    note: str

    def __str__(self) -> str:
        exp = f"{self.expected_interval_minutes:.1f}m" if self.expected_interval_minutes else "N/A"
        actual = f"{self.actual_last_gap_minutes:.1f}m" if self.actual_last_gap_minutes else "N/A"
        return (
            f"[{self.status.upper()}] {self.pipeline} "
            f"expected={exp} last_gap={actual} runs={self.run_count} — {self.note}"
        )


def _sorted_starts(runs: List[PipelineRun]) -> List[datetime]:
    return sorted(
        (r.started_at for r in runs if r.started_at is not None),
        key=lambda dt: dt,
    )


def compute_cadence(
    runs: List[PipelineRun],
    pipeline: str,
    overdue_factor: float = 2.0,
    frequent_factor: float = 0.4,
    min_runs: int = 3,
    now: Optional[datetime] = None,
) -> CadenceResult:
    """Analyse the scheduling cadence of a single pipeline."""
    if now is None:
        now = datetime.now(timezone.utc)

    pipeline_runs = [r for r in runs if r.pipeline == pipeline]
    starts = _sorted_starts(pipeline_runs)

    if len(starts) < min_runs:
        return CadenceResult(
            pipeline=pipeline,
            expected_interval_minutes=None,
            actual_last_gap_minutes=None,
            run_count=len(starts),
            status="insufficient_data",
            note=f"Need at least {min_runs} runs to infer cadence.",
        )

    gaps_minutes = [
        (starts[i + 1] - starts[i]).total_seconds() / 60
        for i in range(len(starts) - 1)
    ]
    expected = mean(gaps_minutes)

    last_start = starts[-1]
    last_gap = (now - last_start).total_seconds() / 60

    if last_gap > expected * overdue_factor:
        status = "overdue"
        note = f"Last run {last_gap:.1f}m ago; expected every {expected:.1f}m."
    elif last_gap < expected * frequent_factor and len(starts) > 1:
        status = "too_frequent"
        note = f"Runs arriving faster than expected ({last_gap:.1f}m < {expected * frequent_factor:.1f}m)."
    else:
        status = "on_time"
        note = "Running within expected cadence window."

    return CadenceResult(
        pipeline=pipeline,
        expected_interval_minutes=round(expected, 2),
        actual_last_gap_minutes=round(last_gap, 2),
        run_count=len(starts),
        status=status,
        note=note,
    )


def compute_all_cadences(
    runs: List[PipelineRun],
    **kwargs,
) -> List[CadenceResult]:
    """Compute cadence for every distinct pipeline in *runs*."""
    pipelines = sorted({r.pipeline for r in runs})
    return [compute_cadence(runs, p, **kwargs) for p in pipelines]
