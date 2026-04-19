"""Hourly/daily rollup aggregation for pipeline runs."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List
from pipewatch.models import PipelineRun


@dataclass
class RollupBucket:
    period: str  # e.g. "2024-01-15" or "2024-01-15T14"
    pipeline: str
    total: int
    failures: int
    successes: int
    avg_duration: float | None

    @property
    def success_rate(self) -> float:
        return self.successes / self.total if self.total else 0.0

    def __str__(self) -> str:
        dur = f"{self.avg_duration:.1f}s" if self.avg_duration is not None else "n/a"
        return (
            f"[{self.period}] {self.pipeline}: "
            f"{self.successes}/{self.total} ok, avg={dur}"
        )


def _bucket_key(run: PipelineRun, granularity: str) -> str:
    dt = datetime.fromtimestamp(run.started_at, tz=timezone.utc)
    if granularity == "hourly":
        return dt.strftime("%Y-%m-%dT%H")
    return dt.strftime("%Y-%m-%d")


def compute_rollup(
    runs: List[PipelineRun], granularity: str = "daily"
) -> Dict[str, Dict[str, RollupBucket]]:
    """Return {period: {pipeline: RollupBucket}}."""
    if granularity not in ("daily", "hourly"):
        raise ValueError(f"granularity must be 'daily' or 'hourly', got {granularity!r}")

    buckets: Dict[str, Dict[str, dict]] = {}
    for run in runs:
        period = _bucket_key(run, granularity)
        buckets.setdefault(period, {}).setdefault(
            run.pipeline,
            {"total": 0, "failures": 0, "successes": 0, "durations": []},
        )
        b = buckets[period][run.pipeline]
        b["total"] += 1
        if run.is_failed():
            b["failures"] += 1
        elif run.is_success():
            b["successes"] += 1
        if run.duration is not None:
            b["durations"].append(run.duration)

    result: Dict[str, Dict[str, RollupBucket]] = {}
    for period, pipes in buckets.items():
        result[period] = {}
        for pipeline, b in pipes.items():
            durs = b["durations"]
            result[period][pipeline] = RollupBucket(
                period=period,
                pipeline=pipeline,
                total=b["total"],
                failures=b["failures"],
                successes=b["successes"],
                avg_duration=sum(durs) / len(durs) if durs else None,
            )
    return result
