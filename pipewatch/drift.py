"""Detect configuration or behavioral drift between pipeline runs over time."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class DriftResult:
    pipeline: str
    metric: str
    baseline_value: float
    current_value: float
    delta: float
    pct_change: float
    flagged: bool

    def __str__(self) -> str:
        direction = "▲" if self.delta > 0 else "▼"
        flag = " [DRIFT]" if self.flagged else ""
        return (
            f"{self.pipeline} | {self.metric}: "
            f"{self.baseline_value:.2f} → {self.current_value:.2f} "
            f"({direction}{abs(self.pct_change):.1f}%){flag}"
        )


def _mean(values: List[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def _success_rate(runs: List[PipelineRun]) -> Optional[float]:
    if not runs:
        return None
    return sum(1 for r in runs if r.is_success()) / len(runs)


def detect_drift(
    runs: List[PipelineRun],
    pipeline: str,
    window_size: int = 10,
    threshold_pct: float = 20.0,
) -> List[DriftResult]:
    """Compare the oldest window_size runs against the most recent window_size runs."""
    pipeline_runs = sorted(
        [r for r in runs if r.pipeline == pipeline],
        key=lambda r: r.started_at,
    )

    if len(pipeline_runs) < window_size * 2:
        return []

    baseline = pipeline_runs[:window_size]
    current = pipeline_runs[-window_size:]

    results: List[DriftResult] = []

    # Success rate drift
    base_sr = _success_rate(baseline)
    curr_sr = _success_rate(current)
    if base_sr is not None and curr_sr is not None and base_sr > 0:
        delta = curr_sr - base_sr
        pct = (delta / base_sr) * 100
        results.append(DriftResult(
            pipeline=pipeline,
            metric="success_rate",
            baseline_value=round(base_sr, 4),
            current_value=round(curr_sr, 4),
            delta=round(delta, 4),
            pct_change=round(pct, 2),
            flagged=abs(pct) >= threshold_pct,
        ))

    # Duration drift
    base_durs = [r.duration for r in baseline if r.duration is not None]
    curr_durs = [r.duration for r in current if r.duration is not None]
    base_avg = _mean(base_durs)
    curr_avg = _mean(curr_durs)
    if base_avg is not None and curr_avg is not None and base_avg > 0:
        delta = curr_avg - base_avg
        pct = (delta / base_avg) * 100
        results.append(DriftResult(
            pipeline=pipeline,
            metric="avg_duration",
            baseline_value=round(base_avg, 2),
            current_value=round(curr_avg, 2),
            delta=round(delta, 2),
            pct_change=round(pct, 2),
            flagged=abs(pct) >= threshold_pct,
        ))

    return results


def detect_all_drift(
    runs: List[PipelineRun],
    window_size: int = 10,
    threshold_pct: float = 20.0,
) -> List[DriftResult]:
    pipelines = sorted({r.pipeline for r in runs})
    results: List[DriftResult] = []
    for p in pipelines:
        results.extend(detect_drift(runs, p, window_size, threshold_pct))
    return results
