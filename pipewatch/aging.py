"""Aging analysis: track how long failed runs have gone unresolved."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class AgingResult:
    pipeline: str
    run_id: str
    failed_at: datetime
    age_minutes: float
    error: Optional[str]

    def __str__(self) -> str:
        age_h = self.age_minutes / 60
        err = self.error or "(no error)"
        return (
            f"{self.pipeline} | run={self.run_id} | "
            f"age={age_h:.1f}h | error={err}"
        )

    @property
    def age_hours(self) -> float:
        return self.age_minutes / 60

    @property
    def severity(self) -> str:
        if self.age_minutes >= 1440:  # 24 h
            return "critical"
        if self.age_minutes >= 360:   # 6 h
            return "warning"
        return "info"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def detect_aging(
    runs: List[PipelineRun],
    min_age_minutes: float = 30.0,
    pipeline: Optional[str] = None,
) -> List[AgingResult]:
    """Return failed runs that have been unresolved for at least *min_age_minutes*."""
    now = _now()
    results: List[AgingResult] = []

    failed = [
        r for r in runs
        if r.is_failed and (pipeline is None or r.pipeline == pipeline)
    ]

    # Keep only the *latest* failed run per pipeline (most relevant)
    latest: dict[str, PipelineRun] = {}
    for r in failed:
        if r.pipeline not in latest or r.started_at > latest[r.pipeline].started_at:
            latest[r.pipeline] = r

    for r in latest.values():
        started = r.started_at
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        age = (now - started).total_seconds() / 60.0
        if age >= min_age_minutes:
            results.append(
                AgingResult(
                    pipeline=r.pipeline,
                    run_id=r.run_id,
                    failed_at=started,
                    age_minutes=age,
                    error=r.error,
                )
            )

    results.sort(key=lambda x: x.age_minutes, reverse=True)
    return results
