"""Mean Time To Recovery (MTTR) analysis for pipeline runs."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.models import PipelineRun, PipelineStatus


@dataclass
class MTTRResult:
    pipeline: str
    total_incidents: int
    recovered_incidents: int
    mean_recovery_minutes: Optional[float]
    longest_recovery_minutes: Optional[float]
    shortest_recovery_minutes: Optional[float]

    def __str__(self) -> str:
        if self.mean_recovery_minutes is None:
            return f"{self.pipeline}: no recovery data"
        return (
            f"{self.pipeline}: MTTR={self.mean_recovery_minutes:.1f}m "
            f"(incidents={self.total_incidents}, "
            f"recovered={self.recovered_incidents}, "
            f"min={self.shortest_recovery_minutes:.1f}m, "
            f"max={self.longest_recovery_minutes:.1f}m)"
        )


def _recovery_minutes(failure: PipelineRun, recovery: PipelineRun) -> float:
    """Minutes between the start of a failure and the start of the next success."""
    delta = recovery.started_at - failure.started_at
    return delta.total_seconds() / 60.0


def compute_mttr(runs: List[PipelineRun], pipeline: str) -> Optional[MTTRResult]:
    """Compute MTTR for a single pipeline from a list of runs."""
    pipeline_runs = sorted(
        [r for r in runs if r.pipeline == pipeline],
        key=lambda r: r.started_at,
    )
    if not pipeline_runs:
        return None

    recovery_times: List[float] = []
    total_incidents = 0
    i = 0
    while i < len(pipeline_runs):
        if pipeline_runs[i].is_failed():
            total_incidents += 1
            # look for next success
            j = i + 1
            while j < len(pipeline_runs) and pipeline_runs[j].is_failed():
                j += 1
            if j < len(pipeline_runs) and pipeline_runs[j].is_success():
                recovery_times.append(
                    _recovery_minutes(pipeline_runs[i], pipeline_runs[j])
                )
            i = j
        else:
            i += 1

    mean_r = sum(recovery_times) / len(recovery_times) if recovery_times else None
    longest_r = max(recovery_times) if recovery_times else None
    shortest_r = min(recovery_times) if recovery_times else None

    return MTTRResult(
        pipeline=pipeline,
        total_incidents=total_incidents,
        recovered_incidents=len(recovery_times),
        mean_recovery_minutes=mean_r,
        longest_recovery_minutes=longest_r,
        shortest_recovery_minutes=shortest_r,
    )


def compute_all_mttr(
    runs: List[PipelineRun],
    pipeline: Optional[str] = None,
) -> List[MTTRResult]:
    """Compute MTTR for all pipelines (or a single one if specified)."""
    if pipeline:
        pipelines = [pipeline]
    else:
        pipelines = sorted({r.pipeline for r in runs})

    results = []
    for p in pipelines:
        result = compute_mttr(runs, p)
        if result is not None:
            results.append(result)
    return results
