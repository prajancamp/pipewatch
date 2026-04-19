"""Patch/update utilities for modifying stored pipeline runs."""

from __future__ import annotations
from typing import Optional
from pipewatch.store import RunStore
from pipewatch.models import PipelineRun


def patch_run(
    store: RunStore,
    run_id: str,
    *,
    error: Optional[str] = None,
    tags: Optional[list] = None,
    meta: Optional[dict] = None,
) -> Optional[PipelineRun]:
    """Update fields on a run by run_id. Returns updated run or None if not found."""
    runs = store.load_all()
    updated = None
    new_runs = []
    for run in runs:
        if run.run_id == run_id:
            d = run.to_dict()
            if error is not None:
                d["error"] = error
            if tags is not None:
                d["tags"] = tags
            if meta is not None:
                existing = d.get("meta") or {}
                existing.update(meta)
                d["meta"] = existing
            run = PipelineRun.from_dict(d)
            updated = run
        new_runs.append(run)
    if updated is not None:
        store.replace_all(new_runs)
    return updated


def delete_run(store: RunStore, run_id: str) -> bool:
    """Remove a run by run_id. Returns True if a run was deleted."""
    runs = store.load_all()
    new_runs = [r for r in runs if r.run_id != run_id]
    if len(new_runs) == len(runs):
        return False
    store.replace_all(new_runs)
    return True


def patch_runs_by_pipeline(
    store: RunStore,
    pipeline: str,
    meta: dict,
) -> int:
    """Merge meta fields into all runs for a given pipeline. Returns count patched."""
    runs = store.load_all()
    count = 0
    new_runs = []
    for run in runs:
        if run.pipeline == pipeline:
            d = run.to_dict()
            existing = d.get("meta") or {}
            existing.update(meta)
            d["meta"] = existing
            run = PipelineRun.from_dict(d)
            count += 1
        new_runs.append(run)
    if count:
        store.replace_all(new_runs)
    return count
