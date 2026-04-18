"""Filtering utilities for pipeline runs."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.models import PipelineRun, PipelineStatus


def filter_runs(
    runs: List[PipelineRun],
    *,
    pipeline: Optional[str] = None,
    status: Optional[PipelineStatus] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> List[PipelineRun]:
    """Return runs matching all supplied criteria."""
    result = runs

    if pipeline is not None:
        result = [r for r in result if r.pipeline_name == pipeline]

    if status is not None:
        result = [r for r in result if r.status == status]

    if since is not None:
        since_utc = since if since.tzinfo else since.replace(tzinfo=timezone.utc)
        result = [r for r in result if r.started_at >= since_utc]

    if until is not None:
        until_utc = until if until.tzinfo else until.replace(tzinfo=timezone.utc)
        result = [r for r in result if r.started_at <= until_utc]

    return result


def unique_pipelines(runs: List[PipelineRun]) -> List[str]:
    """Return sorted list of distinct pipeline names."""
    return sorted({r.pipeline_name for r in runs})


def latest_run_per_pipeline(runs: List[PipelineRun]) -> List[PipelineRun]:
    """Return the most recent run for each pipeline."""
    latest: dict[str, PipelineRun] = {}
    for run in runs:
        existing = latest.get(run.pipeline_name)
        if existing is None or run.started_at > existing.started_at:
            latest[run.pipeline_name] = run
    return sorted(latest.values(), key=lambda r: r.pipeline_name)
