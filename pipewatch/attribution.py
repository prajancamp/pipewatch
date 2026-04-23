"""Attribution: trace which team/owner is responsible for each pipeline."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.models import PipelineRun


@dataclass
class AttributionEntry:
    pipeline: str
    owner: Optional[str]
    team: Optional[str]
    total_runs: int
    failed_runs: int
    success_rate: float

    def __str__(self) -> str:
        owner_str = self.owner or "unknown"
        team_str = self.team or "unknown"
        return (
            f"{self.pipeline} | owner={owner_str} team={team_str} "
            f"runs={self.total_runs} failures={self.failed_runs} "
            f"success={self.success_rate:.0%}"
        )


def _extract_owner(run: PipelineRun) -> Optional[str]:
    if run.meta:
        return run.meta.get("owner") or run.meta.get("owner_email")
    return None


def _extract_team(run: PipelineRun) -> Optional[str]:
    if run.meta:
        return run.meta.get("team") or run.meta.get("squad")
    return None


def attribute_runs(
    runs: List[PipelineRun],
    pipeline: Optional[str] = None,
) -> List[AttributionEntry]:
    """Group runs by pipeline and surface owner/team from meta fields."""
    if pipeline:
        runs = [r for r in runs if r.pipeline == pipeline]

    grouped: Dict[str, List[PipelineRun]] = {}
    for run in runs:
        grouped.setdefault(run.pipeline, []).append(run)

    results: List[AttributionEntry] = []
    for pipe, pipe_runs in sorted(grouped.items()):
        total = len(pipe_runs)
        failed = sum(1 for r in pipe_runs if r.is_failed())
        rate = (total - failed) / total if total else 0.0

        # Use most recent run's meta for owner/team
        sorted_runs = sorted(pipe_runs, key=lambda r: r.started_at or "", reverse=True)
        latest = sorted_runs[0]
        owner = _extract_owner(latest)
        team = _extract_team(latest)

        results.append(
            AttributionEntry(
                pipeline=pipe,
                owner=owner,
                team=team,
                total_runs=total,
                failed_runs=failed,
                success_rate=rate,
            )
        )
    return results


def attribution_by_team(
    entries: List[AttributionEntry],
) -> Dict[str, List[AttributionEntry]]:
    """Group attribution entries by team name."""
    result: Dict[str, List[AttributionEntry]] = {}
    for entry in entries:
        key = entry.team or "unknown"
        result.setdefault(key, []).append(entry)
    return result
