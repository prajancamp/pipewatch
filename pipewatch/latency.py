"""Latency tracking: measure and report p50/p95/p99 run durations per pipeline."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pipewatch.models import PipelineRun


@dataclass
class LatencyResult:
    pipeline: str
    sample_size: int
    p50: Optional[float]
    p95: Optional[float]
    p99: Optional[float]
    max_duration: Optional[float]
    min_duration: Optional[float]

    def __str__(self) -> str:
        if self.p50 is None:
            return f"{self.pipeline}: no duration data"
        return (
            f"{self.pipeline}: "
            f"p50={self.p50:.1f}s  "
            f"p95={self.p95:.1f}s  "
            f"p99={self.p99:.1f}s  "
            f"min={self.min_duration:.1f}s  "
            f"max={self.max_duration:.1f}s  "
            f"(n={self.sample_size})"
        )


def _percentile(sorted_values: List[float], pct: float) -> float:
    """Return the pct-th percentile (0-100) of a pre-sorted list."""
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * pct / 100.0
    lo = int(k)
    hi = lo + 1
    if hi >= len(sorted_values):
        return sorted_values[lo]
    frac = k - lo
    return sorted_values[lo] + frac * (sorted_values[hi] - sorted_values[lo])


def compute_latency(runs: List[PipelineRun], pipeline: str) -> LatencyResult:
    """Compute latency percentiles for a single pipeline."""
    relevant = [r for r in runs if r.pipeline == pipeline and r.duration is not None]
    if not relevant:
        return LatencyResult(
            pipeline=pipeline,
            sample_size=0,
            p50=None, p95=None, p99=None,
            max_duration=None, min_duration=None,
        )
    durations = sorted(r.duration for r in relevant)  # type: ignore[arg-type]
    return LatencyResult(
        pipeline=pipeline,
        sample_size=len(durations),
        p50=_percentile(durations, 50),
        p95=_percentile(durations, 95),
        p99=_percentile(durations, 99),
        min_duration=durations[0],
        max_duration=durations[-1],
    )


def compute_all_latencies(
    runs: List[PipelineRun],
    pipeline: Optional[str] = None,
) -> Dict[str, LatencyResult]:
    """Compute latency stats for all pipelines (or a specific one)."""
    pipelines = (
        {pipeline}
        if pipeline
        else {r.pipeline for r in runs}
    )
    return {p: compute_latency(runs, p) for p in sorted(pipelines)}
