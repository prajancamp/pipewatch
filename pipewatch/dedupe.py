"""Deduplication utilities for pipeline runs."""
from __future__ import annotations
from typing import List, Dict
from pipewatch.models import PipelineRun


def dedupe_by_run_id(runs: List[PipelineRun]) -> List[PipelineRun]:
    """Return runs with duplicate run_ids removed (first occurrence wins)."""
    seen: Dict[str, bool] = {}
    result = []
    for run in runs:
        if run.run_id not in seen:
            seen[run.run_id] = True
            result.append(run)
    return result


def dedupe_latest_per_pipeline(runs: List[PipelineRun]) -> List[PipelineRun]:
    """Return only the most recent run per pipeline_name."""
    latest: Dict[str, PipelineRun] = {}
    for run in runs:
        existing = latest.get(run.pipeline_name)
        if existing is None or run.started_at > existing.started_at:
            latest[run.pipeline_name] = run
    return list(latest.values())


def find_duplicate_run_ids(runs: List[PipelineRun]) -> List[str]:
    """Return a list of run_ids that appear more than once."""
    counts: Dict[str, int] = {}
    for run in runs:
        counts[run.run_id] = counts.get(run.run_id, 0) + 1
    return [rid for rid, count in counts.items() if count > 1]
