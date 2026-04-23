"""Detect pipelines that fail recurrently at the same time-of-day slot."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class RecurrenceResult:
    pipeline: str
    hour_slot: int          # 0-23
    failure_count: int
    total_in_slot: int
    failure_rate: float     # 0.0 – 1.0

    def __str__(self) -> str:
        return (
            f"{self.pipeline} | slot {self.hour_slot:02d}:xx "
            f"| {self.failure_count}/{self.total_in_slot} failures "
            f"({self.failure_rate:.0%})"
        )


def _hour(run: PipelineRun) -> int:
    try:
        return datetime.fromisoformat(run.started_at).hour
    except Exception:
        return -1


def detect_recurrence(
    runs: List[PipelineRun],
    min_occurrences: int = 3,
    min_failure_rate: float = 0.5,
    pipeline: Optional[str] = None,
) -> List[RecurrenceResult]:
    """Return slots where failures recur above *min_failure_rate*."""
    if pipeline:
        runs = [r for r in runs if r.pipeline == pipeline]

    # (pipeline, hour) -> [total, failures]
    buckets: dict[tuple[str, int], list[int]] = defaultdict(lambda: [0, 0])
    for run in runs:
        h = _hour(run)
        if h == -1:
            continue
        key = (run.pipeline, h)
        buckets[key][0] += 1
        if run.is_failed():
            buckets[key][1] += 1

    results: List[RecurrenceResult] = []
    for (pipe, hour), (total, failures) in buckets.items():
        if total < min_occurrences:
            continue
        rate = failures / total
        if rate >= min_failure_rate:
            results.append(
                RecurrenceResult(
                    pipeline=pipe,
                    hour_slot=hour,
                    failure_count=failures,
                    total_in_slot=total,
                    failure_rate=rate,
                )
            )

    results.sort(key=lambda r: (-r.failure_rate, r.pipeline, r.hour_slot))
    return results
