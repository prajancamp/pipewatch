"""Identify pipelines that are consistently slow or degrading in duration."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class BottleneckResult:
    pipeline: str
    avg_duration: float
    max_duration: float
    p90_duration: float
    run_count: int
    threshold: float
    is_bottleneck: bool

    def __str__(self) -> str:
        flag = "[BOTTLENECK]" if self.is_bottleneck else "[ok]"
        return (
            f"{flag} {self.pipeline}: avg={self.avg_duration:.1f}s "
            f"p90={self.p90_duration:.1f}s max={self.max_duration:.1f}s "
            f"(threshold={self.threshold:.1f}s, n={self.run_count})"
        )


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    idx = int(len(sorted_vals) * pct / 100)
    idx = min(idx, len(sorted_vals) - 1)
    return sorted_vals[idx]


def detect_bottlenecks(
    runs: List[PipelineRun],
    threshold: float = 300.0,
    min_runs: int = 3,
    pipeline: Optional[str] = None,
) -> List[BottleneckResult]:
    """Detect pipelines whose p90 duration exceeds *threshold* seconds.

    Args:
        runs: All pipeline runs to analyse.
        threshold: Duration in seconds above which a pipeline is flagged.
        min_runs: Minimum completed runs required to evaluate a pipeline.
        pipeline: If set, restrict analysis to this pipeline name.
    """
    from collections import defaultdict

    buckets: dict[str, List[float]] = defaultdict(list)
    for run in runs:
        if pipeline and run.pipeline != pipeline:
            continue
        if run.duration is not None and run.duration >= 0:
            buckets[run.pipeline].append(run.duration)

    results: List[BottleneckResult] = []
    for name, durations in sorted(buckets.items()):
        if len(durations) < min_runs:
            continue
        avg = sum(durations) / len(durations)
        p90 = _percentile(durations, 90)
        results.append(
            BottleneckResult(
                pipeline=name,
                avg_duration=avg,
                max_duration=max(durations),
                p90_duration=p90,
                run_count=len(durations),
                threshold=threshold,
                is_bottleneck=p90 > threshold,
            )
        )
    return results
