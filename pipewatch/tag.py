"""Tag-based filtering and grouping for pipeline runs."""
from typing import Dict, List
from collections import defaultdict
from pipewatch.models import PipelineRun


def runs_with_tag(runs: List[PipelineRun], tag: str) -> List[PipelineRun]:
    """Return runs that have the given tag."""
    return [r for r in runs if tag in (r.tags or [])]


def runs_without_tag(runs: List[PipelineRun], tag: str) -> List[PipelineRun]:
    """Return runs that do NOT have the given tag."""
    return [r for r in runs if tag not in (r.tags or [])]


def group_by_tag(runs: List[PipelineRun]) -> Dict[str, List[PipelineRun]]:
    """Group runs by each tag they carry. A run may appear under multiple tags."""
    groups: Dict[str, List[PipelineRun]] = defaultdict(list)
    for run in runs:
        for tag in (run.tags or []):
            groups[tag].append(run)
    return dict(groups)


def all_tags(runs: List[PipelineRun]) -> List[str]:
    """Return sorted list of unique tags across all runs."""
    seen = set()
    for run in runs:
        for tag in (run.tags or []):
            seen.add(tag)
    return sorted(seen)


def filter_by_tags(runs: List[PipelineRun], tags: List[str], match_all: bool = False) -> List[PipelineRun]:
    """Filter runs by a list of tags.

    Args:
        runs: list of pipeline runs
        tags: tags to match
        match_all: if True, run must have ALL tags; if False, ANY tag suffices
    """
    if not tags:
        return runs
    tag_set = set(tags)
    result = []
    for run in runs:
        run_tags = set(run.tags or [])
        if match_all:
            if tag_set.issubset(run_tags):
                result.append(run)
        else:
            if tag_set & run_tags:
                result.append(run)
    return result
