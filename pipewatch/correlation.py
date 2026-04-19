"""Correlate pipeline failures across pipelines by time proximity."""
from dataclasses import dataclass, field
from typing import List, Dict, Tuple
from datetime import datetime, timedelta
from pipewatch.models import PipelineRun


@dataclass
class CorrelationResult:
    pipeline_a: str
    pipeline_b: str
    co_failures: int
    window_minutes: int

    def __str__(self) -> str:
        return (
            f"{self.pipeline_a} <-> {self.pipeline_b}: "
            f"{self.co_failures} co-failure(s) within {self.window_minutes}m"
        )


def _failed_times(runs: List[PipelineRun], pipeline: str) -> List[datetime]:
    return [
        r.started_at
        for r in runs
        if r.pipeline == pipeline and r.is_failed() and r.started_at
    ]


def compute_correlations(
    runs: List[PipelineRun],
    window_minutes: int = 5,
) -> List[CorrelationResult]:
    """Find pairs of pipelines that tend to fail close together in time."""
    from pipewatch.filter import unique_pipelines

    pipelines = sorted(unique_pipelines(runs))
    window = timedelta(minutes=window_minutes)
    results: List[CorrelationResult] = []

    for i, pa in enumerate(pipelines):
        for pb in pipelines[i + 1 :]:
            times_a = _failed_times(runs, pa)
            times_b = _failed_times(runs, pb)
            co = 0
            for ta in times_a:
                for tb in times_b:
                    if abs((ta - tb).total_seconds()) <= window.total_seconds():
                        co += 1
                        break
            if co > 0:
                results.append(CorrelationResult(pa, pb, co, window_minutes))

    results.sort(key=lambda r: -r.co_failures)
    return results
