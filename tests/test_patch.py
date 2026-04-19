"""Tests for pipewatch.patch module."""

import pytest
from pathlib import Path
from datetime import datetime, timezone
from pipewatch.store import RunStore
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.patch import patch_run, delete_run, patch_runs_by_pipeline


@pytest.fixture
def tmp_store(tmp_path):
    return RunStore(tmp_path / "runs.jsonl")


def make_run(run_id="r1", pipeline="etl", status=PipelineStatus.SUCCESS, error=None, tags=None, meta=None):
    return PipelineRun(
        run_id=run_id,
        pipeline=pipeline,
        status=status,
        started_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ended_at=datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc),
        error=error,
        tags=tags or [],
        meta=meta or {},
    )


def test_patch_run_updates_error(tmp_store):
    tmp_store.append(make_run(run_id="r1"))
    result = patch_run(tmp_store, "r1", error="timeout")
    assert result is not None
    assert result.error == "timeout"
    assert tmp_store.load_all()[0].error == "timeout"


def test_patch_run_updates_tags(tmp_store):
    tmp_store.append(make_run(run_id="r2"))
    result = patch_run(tmp_store, "r2", tags=["prod", "nightly"])
    assert result.tags == ["prod", "nightly"]


def test_patch_run_merges_meta(tmp_store):
    tmp_store.append(make_run(run_id="r3", meta={"env": "dev"}))
    result = patch_run(tmp_store, "r3", meta={"owner": "alice"})
    assert result.meta["env"] == "dev"
    assert result.meta["owner"] == "alice"


def test_patch_run_not_found(tmp_store):
    tmp_store.append(make_run(run_id="r1"))
    result = patch_run(tmp_store, "missing", error="x")
    assert result is None


def test_delete_run_removes_it(tmp_store):
    tmp_store.append(make_run(run_id="r1"))
    tmp_store.append(make_run(run_id="r2"))
    deleted = delete_run(tmp_store, "r1")
    assert deleted is True
    ids = [r.run_id for r in tmp_store.load_all()]
    assert "r1" not in ids
    assert "r2" in ids


def test_delete_run_not_found(tmp_store):
    tmp_store.append(make_run(run_id="r1"))
    deleted = delete_run(tmp_store, "ghost")
    assert deleted is False
    assert len(tmp_store.load_all()) == 1


def test_patch_runs_by_pipeline(tmp_store):
    tmp_store.append(make_run(run_id="r1", pipeline="etl"))
    tmp_store.append(make_run(run_id="r2", pipeline="etl"))
    tmp_store.append(make_run(run_id="r3", pipeline="other"))
    count = patch_runs_by_pipeline(tmp_store, "etl", {"team": "data"})
    assert count == 2
    for run in tmp_store.load_all():
        if run.pipeline == "etl":
            assert run.meta.get("team") == "data"
        else:
            assert run.meta.get("team") is None
