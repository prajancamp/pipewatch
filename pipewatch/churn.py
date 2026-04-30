"""Detect pipelines with high failure churn — frequent alternation between
passing and failing states within a rolling window."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class ChurnResult:
    pipeline: str
    window_hours: int
    total_runs: int
    transitions: int          # number of status changes
    churn_rate: float         # transitions / (total_runs - 1), or 0
    is_churning: bool

    def __str__(self) -> str:
        flag = "⚠ CHURNING" if self.is_churning else "OK"
        return (
            f"{self.pipeline}: {self.transitions} transitions over "
            f"{self.total_runs} runs in {self.window_hours}h "
            f"(churn={self.churn_rate:.0%}) [{flag}]"
        )


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _count_transitions(runs: List[PipelineRun]) -> int:
    """Count status-change boundaries in time-ordered runs."""
    ordered = sorted(runs, key=lambda r: r.started_at)
    transitions = 0
    for i in range(1, len(ordered)):
        if ordered[i].status != ordered[i - 1].status:
            transitions += 1
    return transitions


def detect_churn(
    runs: List[PipelineRun],
    window_hours: int = 24,
    min_runs: int = 4,
    churn_threshold: float = 0.5,
    pipeline: Optional[str] = None,
) -> List[ChurnResult]:
    """Return a ChurnResult for every pipeline that meets *min_runs* within
    *window_hours* and has a churn_rate >= *churn_threshold*."""
    cutoff = _now() - timedelta(hours=window_hours)
    recent = [r for r in runs if r.started_at >= cutoff]
    if pipeline:
        recent = [r for r in recent if r.pipeline == pipeline]

    by_pipeline: dict[str, List[PipelineRun]] = {}
    for run in recent:
        by_pipeline.setdefault(run.pipeline, []).append(run)

    results: List[ChurnResult] = []
    for name, pruns in sorted(by_pipeline.items()):
        total = len(pruns)
        if total < min_runs:
            continue
        transitions = _count_transitions(pruns)
        rate = transitions / (total - 1) if total > 1 else 0.0
        results.append(
            ChurnResult(
                pipeline=name,
                window_hours=window_hours,
                total_runs=total,
                transitions=transitions,
                churn_rate=rate,
                is_churning=rate >= churn_threshold,
            )
        )
    return results
