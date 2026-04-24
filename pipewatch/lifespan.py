"""Lifespan analysis: track how long pipelines have been active and flag aging ones."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class LifespanResult:
    pipeline: str
    first_seen: datetime
    last_seen: datetime
    total_runs: int
    age_days: float
    warning: Optional[str] = None

    def __str__(self) -> str:
        warn = f"  ⚠ {self.warning}" if self.warning else ""
        return (
            f"{self.pipeline}: age={self.age_days:.1f}d "
            f"runs={self.total_runs} "
            f"first={self.first_seen.date()} last={self.last_seen.date()}{warn}"
        )


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def compute_lifespan(
    runs: List[PipelineRun],
    pipeline: str,
    warn_after_days: float = 180.0,
) -> Optional[LifespanResult]:
    """Compute lifespan metrics for a single pipeline."""
    pipeline_runs = [r for r in runs if r.pipeline == pipeline]
    if not pipeline_runs:
        return None

    starts = sorted(
        r.started_at for r in pipeline_runs if r.started_at is not None
    )
    if not starts:
        return None

    first_seen = starts[0]
    last_seen = starts[-1]
    now = _now()

    if first_seen.tzinfo is None:
        first_seen = first_seen.replace(tzinfo=timezone.utc)
    if last_seen.tzinfo is None:
        last_seen = last_seen.replace(tzinfo=timezone.utc)

    age_days = (now - first_seen).total_seconds() / 86400.0

    warning: Optional[str] = None
    if age_days >= warn_after_days:
        warning = f"pipeline active for {age_days:.0f} days — consider review"

    return LifespanResult(
        pipeline=pipeline,
        first_seen=first_seen,
        last_seen=last_seen,
        total_runs=len(pipeline_runs),
        age_days=age_days,
        warning=warning,
    )


def compute_all_lifespans(
    runs: List[PipelineRun],
    warn_after_days: float = 180.0,
) -> List[LifespanResult]:
    """Compute lifespan for every distinct pipeline in *runs*."""
    pipelines = sorted({r.pipeline for r in runs})
    results = []
    for p in pipelines:
        result = compute_lifespan(runs, p, warn_after_days=warn_after_days)
        if result is not None:
            results.append(result)
    return results
