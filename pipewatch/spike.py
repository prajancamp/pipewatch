"""Spike detection: identify sudden bursts in failure counts within a rolling window."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class SpikeResult:
    pipeline: str
    window_minutes: int
    baseline_failure_rate: float   # rate over the lookback period
    spike_failure_rate: float      # rate in the recent window
    spike_count: int
    total_recent: int
    flagged: bool

    def __str__(self) -> str:
        status = "SPIKE" if self.flagged else "ok"
        return (
            f"[{status}] {self.pipeline}: "
            f"{self.spike_count}/{self.total_recent} failures in last {self.window_minutes}m "
            f"(baseline={self.baseline_failure_rate:.1%}, spike={self.spike_failure_rate:.1%})"
        )


def _failure_rate(runs: List[PipelineRun]) -> float:
    if not runs:
        return 0.0
    return sum(1 for r in runs if r.is_failed()) / len(runs)


def detect_spikes(
    runs: List[PipelineRun],
    window_minutes: int = 30,
    lookback_minutes: int = 360,
    threshold_multiplier: float = 2.0,
    min_spike_count: int = 2,
    pipeline: Optional[str] = None,
) -> List[SpikeResult]:
    """Detect pipelines whose recent failure rate is significantly above their baseline."""
    now = datetime.now(tz=timezone.utc)
    recent_cutoff = now - timedelta(minutes=window_minutes)
    baseline_cutoff = now - timedelta(minutes=lookback_minutes)

    if pipeline:
        runs = [r for r in runs if r.pipeline == pipeline]

    pipelines = {r.pipeline for r in runs}
    results: List[SpikeResult] = []

    for name in sorted(pipelines):
        pipe_runs = [r for r in runs if r.pipeline == name]
        baseline_runs = [
            r for r in pipe_runs
            if baseline_cutoff <= r.started_at < recent_cutoff
        ]
        recent_runs = [r for r in pipe_runs if r.started_at >= recent_cutoff]

        if not recent_runs:
            continue

        baseline_rate = _failure_rate(baseline_runs)
        spike_rate = _failure_rate(recent_runs)
        spike_count = sum(1 for r in recent_runs if r.is_failed())

        flagged = (
            spike_count >= min_spike_count
            and spike_rate >= threshold_multiplier * max(baseline_rate, 0.05)
        )

        results.append(SpikeResult(
            pipeline=name,
            window_minutes=window_minutes,
            baseline_failure_rate=baseline_rate,
            spike_failure_rate=spike_rate,
            spike_count=spike_count,
            total_recent=len(recent_runs),
            flagged=flagged,
        ))

    return results
