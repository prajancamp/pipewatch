"""Full-text and field search across pipeline runs."""
from __future__ import annotations
from typing import List, Optional
from pipewatch.models import PipelineRun


def search_by_error(runs: List[PipelineRun], query: str, case_sensitive: bool = False) -> List[PipelineRun]:
    """Return runs whose error message contains query."""
    if not case_sensitive:
        query = query.lower()
    results = []
    for run in runs:
        err = run.error or ""
        if not case_sensitive:
            err = err.lower()
        if query in err:
            results.append(run)
    return results


def search_by_pipeline(runs: List[PipelineRun], query: str, case_sensitive: bool = False) -> List[PipelineRun]:
    """Return runs whose pipeline name contains query."""
    if not case_sensitive:
        query = query.lower()
    return [
        r for r in runs
        if query in (r.pipeline_name if case_sensitive else r.pipeline_name.lower())
    ]


def search_runs(
    runs: List[PipelineRun],
    query: str,
    fields: Optional[List[str]] = None,
    case_sensitive: bool = False,
) -> List[PipelineRun]:
    """Search runs across specified fields (default: pipeline_name, error)."""
    if fields is None:
        fields = ["pipeline_name", "error"]
    q = query if case_sensitive else query.lower()
    results = []
    for run in runs:
        for field in fields:
            val = getattr(run, field, None) or ""
            if not case_sensitive:
                val = val.lower()
            if q in val:
                results.append(run)
                break
    return results
