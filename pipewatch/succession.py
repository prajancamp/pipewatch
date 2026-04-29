"""Succession analysis: detect which pipelines consistently follow failures
of another pipeline, suggesting a causal or dependency relationship."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pipewatch.models import PipelineRun


@dataclass
class SuccessionResult:
    """A pipeline that frequently runs (and fails) after another pipeline fails."""
    trigger_pipeline: str
    successor_pipeline: str
    trigger_failures: int
    successor_failures_after: int
    rate: float  # fraction of trigger failures followed by a successor failure

    def __str__(self) -> str:
        return (
            f"{self.successor_pipeline} fails after {self.trigger_pipeline} "
            f"{self.successor_failures_after}/{self.trigger_failures} times "
            f"({self.rate:.0%})"
        )


def _failed_start_times(runs: List[PipelineRun], pipeline: str) -> List[float]:
    """Return sorted start timestamps (epoch) for failed runs of a pipeline."""
    times = [
        r.started_at.timestamp()
        for r in runs
        if r.pipeline == pipeline and r.is_failed()
        and r.started_at is not None
    ]
    return sorted(times)


def detect_succession(
    runs: List[PipelineRun],
    window_seconds: float = 300.0,
    min_rate: float = 0.5,
    min_occurrences: int = 2,
    pipeline: Optional[str] = None,
) -> List[SuccessionResult]:
    """For each pair of distinct pipelines, check whether failures in the
    successor tend to occur within *window_seconds* after a failure in the
    trigger pipeline.

    Args:
        runs: All pipeline runs to analyse.
        window_seconds: How many seconds after a trigger failure to look.
        min_rate: Minimum fraction of trigger failures that must be followed
            by a successor failure to surface the result.
        min_occurrences: Minimum absolute number of co-occurrences required.
        pipeline: If set, only return results where trigger equals this value.

    Returns:
        List of SuccessionResult sorted by rate descending.
    """
    pipelines = sorted({r.pipeline for r in runs})
    results: List[SuccessionResult] = []

    triggers = [pipeline] if pipeline else pipelines

    for trigger in triggers:
        trigger_times = _failed_start_times(runs, trigger)
        if not trigger_times:
            continue

        for successor in pipelines:
            if successor == trigger:
                continue

            successor_times = _failed_start_times(runs, successor)
            if not successor_times:
                continue

            count = 0
            for t in trigger_times:
                # Check if any successor failure falls in (t, t + window]
                for s in successor_times:
                    if t < s <= t + window_seconds:
                        count += 1
                        break

            if count < min_occurrences:
                continue

            rate = count / len(trigger_times)
            if rate < min_rate:
                continue

            results.append(SuccessionResult(
                trigger_pipeline=trigger,
                successor_pipeline=successor,
                trigger_failures=len(trigger_times),
                successor_failures_after=count,
                rate=rate,
            ))

    results.sort(key=lambda r: r.rate, reverse=True)
    return results
