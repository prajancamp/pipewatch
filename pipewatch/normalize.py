"""Normalize pipeline run fields for consistent downstream processing."""

from __future__ import annotations

import re
from typing import List

from pipewatch.models import PipelineRun


def normalize_pipeline_name(name: str) -> str:
    """Lowercase, strip whitespace, replace spaces/dashes with underscores."""
    name = name.strip().lower()
    name = re.sub(r"[\s\-]+", "_", name)
    name = re.sub(r"[^a-z0-9_]", "", name)
    return name


def normalize_error(error: str | None) -> str | None:
    """Strip and truncate error messages; return None if blank."""
    if error is None:
        return None
    error = error.strip()
    if not error:
        return None
    return error[:500]


def normalize_tags(tags: list | None) -> List[str]:
    """Deduplicate, lowercase, and sort tags."""
    if not tags:
        return []
    cleaned = sorted(set(t.strip().lower() for t in tags if t and t.strip()))
    return cleaned


def normalize_run(run: PipelineRun) -> PipelineRun:
    """Return a new PipelineRun with normalized fields."""
    import dataclasses

    updates = {
        "pipeline": normalize_pipeline_name(run.pipeline),
        "error": normalize_error(run.error),
        "tags": normalize_tags(run.tags),
    }
    return dataclasses.replace(run, **updates)


def normalize_runs(runs: List[PipelineRun]) -> List[PipelineRun]:
    """Normalize a list of pipeline runs."""
    return [normalize_run(r) for r in runs]
