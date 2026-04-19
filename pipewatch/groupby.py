from __future__ import annotations
from collections import defaultdict
from typing import Dict, List, Optional
from pipewatch.models import PipelineRun


def group_by_status(runs: List[PipelineRun]) -> Dict[str, List[PipelineRun]]:
    """Group runs by their status string."""
    groups: Dict[str, List[PipelineRun]] = defaultdict(list)
    for run in runs:
        groups[run.status.value].append(run)
    return dict(groups)


def group_by_pipeline(runs: List[PipelineRun]) -> Dict[str, List[PipelineRun]]:
    """Group runs by pipeline name."""
    groups: Dict[str, List[PipelineRun]] = defaultdict(list)
    for run in runs:
        groups[run.pipeline_name].append(run)
    return dict(groups)


def group_by_date(runs: List[PipelineRun], fmt: str = "%Y-%m-%d") -> Dict[str, List[PipelineRun]]:
    """Group runs by start_time date using the given strftime format."""
    groups: Dict[str, List[PipelineRun]] = defaultdict(list)
    for run in runs:
        if run.start_time is None:
            key = "unknown"
        else:
            key = run.start_time.strftime(fmt)
        groups[key].append(run)
    return dict(groups)


def group_by_meta_field(runs: List[PipelineRun], field: str) -> Dict[str, List[PipelineRun]]:
    """Group runs by an arbitrary meta dict field. Missing values go under '__missing__'."""
    groups: Dict[str, List[PipelineRun]] = defaultdict(list)
    for run in runs:
        value = (run.meta or {}).get(field)
        key = str(value) if value is not None else "__missing__"
        groups[key].append(run)
    return dict(groups)


def counts(groups: Dict[str, List[PipelineRun]]) -> Dict[str, int]:
    """Return a dict of group key -> count of runs."""
    return {k: len(v) for k, v in groups.items()}
