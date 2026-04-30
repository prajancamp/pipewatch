"""Pareto analysis: identify the 20% of pipelines causing 80% of failures."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class ParetoEntry:
    pipeline: str
    failure_count: int
    total_count: int
    cumulative_failure_pct: float  # running % of all failures up to this row

    def failure_rate(self) -> float:
        if self.total_count == 0:
            return 0.0
        return self.failure_count / self.total_count

    def __str__(self) -> str:
        return (
            f"{self.pipeline}: {self.failure_count} failures "
            f"({self.failure_rate():.0%} rate, "
            f"cumulative {self.cumulative_failure_pct:.0%} of all failures)"
        )


def compute_pareto(
    runs: List[PipelineRun],
    pipeline: Optional[str] = None,
    threshold: float = 0.8,
) -> List[ParetoEntry]:
    """Return pipelines sorted by failure count descending.

    ``threshold`` is used only to mark which entries fall within the Pareto
    boundary; all entries are always returned so callers can render a full
    table.  The returned list is sorted by failure_count descending.
    """
    if pipeline:
        runs = [r for r in runs if r.pipeline == pipeline]

    counts: dict[str, dict[str, int]] = {}
    for run in runs:
        entry = counts.setdefault(run.pipeline, {"failures": 0, "total": 0})
        entry["total"] += 1
        if run.is_failed():
            entry["failures"] += 1

    if not counts:
        return []

    total_failures = sum(v["failures"] for v in counts.values())
    if total_failures == 0:
        # No failures at all — still return entries sorted by pipeline name
        return [
            ParetoEntry(
                pipeline=p,
                failure_count=0,
                total_count=v["total"],
                cumulative_failure_pct=0.0,
            )
            for p, v in sorted(counts.items())
        ]

    sorted_entries = sorted(
        counts.items(), key=lambda kv: kv[1]["failures"], reverse=True
    )

    result: List[ParetoEntry] = []
    cumulative = 0
    for p, v in sorted_entries:
        cumulative += v["failures"]
        result.append(
            ParetoEntry(
                pipeline=p,
                failure_count=v["failures"],
                total_count=v["total"],
                cumulative_failure_pct=cumulative / total_failures,
            )
        )
    return result


def pareto_boundary(entries: List[ParetoEntry], threshold: float = 0.8) -> List[ParetoEntry]:
    """Return only the entries that together account for *threshold* of failures."""
    return [e for e in entries if e.cumulative_failure_pct <= threshold + 1e-9]
