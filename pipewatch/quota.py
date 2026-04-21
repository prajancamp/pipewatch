"""Pipeline run quota tracking — flag pipelines exceeding expected run counts."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class QuotaResult:
    pipeline: str
    expected_max: int
    actual_count: int
    breaching: bool
    window_hours: int

    def __str__(self) -> str:
        status = "BREACH" if self.breaching else "OK"
        return (
            f"[{status}] {self.pipeline}: {self.actual_count} runs "
            f"in last {self.window_hours}h (max {self.expected_max})"
        )


def _runs_in_window(runs: List[PipelineRun], pipeline: str, window_hours: int) -> List[PipelineRun]:
    """Return runs for a given pipeline within the last window_hours."""
    import datetime

    cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=window_hours)
    cutoff_iso = cutoff.isoformat()
    return [
        r for r in runs
        if r.pipeline == pipeline and r.started_at >= cutoff_iso
    ]


def check_quota(
    runs: List[PipelineRun],
    expected_max: int,
    window_hours: int = 24,
    pipeline: Optional[str] = None,
) -> List[QuotaResult]:
    """Check whether any pipeline exceeds expected_max runs in window_hours."""
    pipelines = {r.pipeline for r in runs}
    if pipeline:
        pipelines = {p for p in pipelines if p == pipeline}

    results: List[QuotaResult] = []
    for name in sorted(pipelines):
        window_runs = _runs_in_window(runs, name, window_hours)
        count = len(window_runs)
        results.append(
            QuotaResult(
                pipeline=name,
                expected_max=expected_max,
                actual_count=count,
                breaching=count > expected_max,
                window_hours=window_hours,
            )
        )
    return results


def breaching_quotas(results: List[QuotaResult]) -> List[QuotaResult]:
    """Filter to only breaching quota results."""
    return [r for r in results if r.breaching]
