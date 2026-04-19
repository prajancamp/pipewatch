"""Retention policy: prune old pipeline runs from the store."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Optional

from pipewatch.models import PipelineRun
from pipewatch.store import RunStore


def prune_before(runs: List[PipelineRun], cutoff: datetime) -> List[PipelineRun]:
    """Return runs whose started_at is on or after *cutoff*."""
    return [r for r in runs if r.started_at >= cutoff]


def prune_by_count(runs: List[PipelineRun], keep: int) -> List[PipelineRun]:
    """Keep only the *keep* most recent runs (by started_at)."""
    if keep <= 0:
        return []
    sorted_runs = sorted(runs, key=lambda r: r.started_at)
    return sorted_runs[-keep:]


def apply_retention(
    store: RunStore,
    max_age_days: Optional[int] = None,
    max_count: Optional[int] = None,
) -> int:
    """Prune the store in-place and return the number of runs removed.

    If both *max_age_days* and *max_count* are given, both filters are applied
    (the more restrictive one wins).
    """
    if max_age_days is None and max_count is None:
        return 0

    runs = store.load_all()
    original = len(runs)

    if max_age_days is not None:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=max_age_days)
        runs = prune_before(runs, cutoff)

    if max_count is not None:
        runs = prune_by_count(runs, max_count)

    # Rewrite store
    store.path.write_text("")  # truncate
    for run in runs:
        store.append(run)

    return original - len(runs)
