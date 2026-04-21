"""Rerun suggestion engine: identifies pipelines that are good candidates for retry."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.models import PipelineRun
from pipewatch.filter import latest_run_per_pipeline, runs_by_status
from pipewatch.analyzer import compute_stats


@dataclass
class RerunCandidate:
    pipeline: str
    last_error: Optional[str]
    consecutive_failures: int
    success_rate: float
    reason: str

    def __str__(self) -> str:
        rate_pct = f"{self.success_rate * 100:.1f}%"
        return (
            f"[RERUN] {self.pipeline} | "
            f"consecutive_failures={self.consecutive_failures} | "
            f"success_rate={rate_pct} | "
            f"reason={self.reason} | "
            f"last_error={self.last_error or 'n/a'}"
        )


def _is_transient_error(error: Optional[str]) -> bool:
    """Heuristic: transient errors often mention timeouts, connections, or temporary issues."""
    if not error:
        return False
    transient_keywords = (
        "timeout", "connection", "temporary", "retry", "unavailable",
        "rate limit", "throttl", "503", "502", "network",
    )
    lower = error.lower()
    return any(kw in lower for kw in transient_keywords)


def suggest_reruns(
    runs: List[PipelineRun],
    max_consecutive: int = 3,
    min_success_rate: float = 0.5,
) -> List[RerunCandidate]:
    """Return pipelines whose latest run failed and appear worth retrying."""
    if not runs:
        return []

    failed_latest = [
        r for r in latest_run_per_pipeline(runs) if r.is_failed()
    ]
    if not failed_latest:
        return []

    stats_map = {s.pipeline: s for s in compute_stats(runs)}
    candidates: List[RerunCandidate] = []

    for run in failed_latest:
        stats = stats_map.get(run.pipeline)
        if stats is None:
            continue

        consecutive = stats.consecutive_failures
        rate = stats.success_rate

        # Skip pipelines that are chronically broken
        if consecutive > max_consecutive and rate < min_success_rate:
            continue

        if _is_transient_error(run.error):
            reason = "transient_error"
        elif consecutive <= 1:
            reason = "isolated_failure"
        elif rate >= min_success_rate:
            reason = "historically_reliable"
        else:
            continue

        candidates.append(
            RerunCandidate(
                pipeline=run.pipeline,
                last_error=run.error,
                consecutive_failures=consecutive,
                success_rate=rate,
                reason=reason,
            )
        )

    return sorted(candidates, key=lambda c: (-c.success_rate, c.pipeline))
