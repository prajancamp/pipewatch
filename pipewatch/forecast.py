"""Simple failure-rate forecasting based on recent trend windows."""

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
from pipewatch.models import PipelineRun
from pipewatch.filter import filter_runs
from datetime import datetime, timezone


@dataclass
class ForecastResult:
    pipeline: str
    recent_failure_rate: float   # last window
    older_failure_rate: float    # prior window
    predicted_failure_rate: float
    confidence: str              # 'low' | 'medium' | 'high'

    def __str__(self) -> str:
        direction = (
            "improving" if self.predicted_failure_rate < self.recent_failure_rate
            else "degrading" if self.predicted_failure_rate > self.recent_failure_rate
            else "stable"
        )
        return (
            f"{self.pipeline}: predicted failure rate {self.predicted_failure_rate:.1%} "
            f"({direction}, confidence={self.confidence})"
        )


def _failure_rate(runs: List[PipelineRun]) -> float:
    if not runs:
        return 0.0
    return sum(1 for r in runs if r.is_failed()) / len(runs)


def _confidence(n: int) -> str:
    if n >= 20:
        return "high"
    if n >= 8:
        return "medium"
    return "low"


def forecast_pipeline(
    runs: List[PipelineRun],
    pipeline: str,
    window: int = 20,
) -> Optional[ForecastResult]:
    """Forecast failure rate for a single pipeline using two windows."""
    pipe_runs = [r for r in runs if r.pipeline == pipeline]
    pipe_runs.sort(key=lambda r: r.started_at)
    if len(pipe_runs) < 4:
        return None

    mid = len(pipe_runs) // 2
    older = pipe_runs[:mid][-window:]
    recent = pipe_runs[mid:][-window:]

    older_rate = _failure_rate(older)
    recent_rate = _failure_rate(recent)
    delta = recent_rate - older_rate
    predicted = max(0.0, min(1.0, recent_rate + delta * 0.5))

    return ForecastResult(
        pipeline=pipeline,
        recent_failure_rate=recent_rate,
        older_failure_rate=older_rate,
        predicted_failure_rate=predicted,
        confidence=_confidence(len(recent)),
    )


def forecast_all(
    runs: List[PipelineRun],
    window: int = 20,
) -> List[ForecastResult]:
    """Return forecasts for every pipeline present in runs."""
    pipelines = list({r.pipeline for r in runs})
    results = []
    for p in sorted(pipelines):
        result = forecast_pipeline(runs, p, window=window)
        if result is not None:
            results.append(result)
    return results
