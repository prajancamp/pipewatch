"""Trend analysis: detect improving/degrading pipeline health over time."""
from dataclasses import dataclass
from typing import List, Optional
from pipewatch.models import PipelineRun


@dataclass
class TrendResult:
    pipeline: str
    window_a_success_rate: float
    window_b_success_rate: float
    delta: float  # positive = improving, negative = degrading
    verdict: str  # 'improving', 'degrading', 'stable'

    def __str__(self) -> str:
        arrow = {"improving": "↑", "degrading": "↓", "stable": "→"}[self.verdict]
        return (
            f"{self.pipeline}: {arrow} {self.verdict} "
            f"({self.window_a_success_rate:.0%} → {self.window_b_success_rate:.0%})"
        )


def _success_rate(runs: List[PipelineRun]) -> float:
    if not runs:
        return 0.0
    return sum(1 for r in runs if r.is_success()) / len(runs)


def compute_trend(
    runs: List[PipelineRun],
    pipeline: str,
    window: int = 5,
    threshold: float = 0.1,
) -> Optional[TrendResult]:
    """Compare success rate of last `window` runs vs previous `window` runs."""
    pipeline_runs = [r for r in runs if r.pipeline == pipeline]
    pipeline_runs.sort(key=lambda r: r.started_at)

    if len(pipeline_runs) < window * 2:
        return None

    recent = pipeline_runs[-window:]
    previous = pipeline_runs[-window * 2:-window]

    rate_a = _success_rate(previous)
    rate_b = _success_rate(recent)
    delta = rate_b - rate_a

    if delta > threshold:
        verdict = "improving"
    elif delta < -threshold:
        verdict = "degrading"
    else:
        verdict = "stable"

    return TrendResult(
        pipeline=pipeline,
        window_a_success_rate=rate_a,
        window_b_success_rate=rate_b,
        delta=delta,
        verdict=verdict,
    )


def compute_all_trends(
    runs: List[PipelineRun],
    window: int = 5,
    threshold: float = 0.1,
) -> List[TrendResult]:
    pipelines = {r.pipeline for r in runs}
    results = []
    for p in sorted(pipelines):
        t = compute_trend(runs, p, window=window, threshold=threshold)
        if t is not None:
            results.append(t)
    return results
