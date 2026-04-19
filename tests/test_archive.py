"""Tests for pipewatch.archive."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.archive import archive_before, load_archive, list_archives
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore


def make_run(name: str, started_at: datetime) -> PipelineRun:
    return PipelineRun(
        run_id=str(uuid.uuid4()),
        pipeline_name=name,
        status=PipelineStatus.SUCCESS,
        started_at=started_at,
        ended_at=started_at + timedelta(seconds=10),
    )


@pytest.fixture()
def tmp_store(tmp_path: Path) -> RunStore:
    store_file = tmp_path / "runs" / "runs.jsonl"
    store_file.parent.mkdir(parents=True)
    return RunStore(str(store_file))


def test_archive_before_moves_old_runs(tmp_store: RunStore) -> None:
    now = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    old = make_run("pipe-a", now - timedelta(days=10))
    recent = make_run("pipe-b", now)
    tmp_store.append(old)
    tmp_store.append(recent)

    cutoff = now - timedelta(days=5)
    dest = archive_before(tmp_store, cutoff)

    archived = load_archive(dest)
    assert len(archived) == 1
    assert archived[0].run_id == old.run_id

    remaining = tmp_store.load_all()
    assert len(remaining) == 1
    assert remaining[0].run_id == recent.run_id


def test_archive_before_empty_result(tmp_store: RunStore) -> None:
    now = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    recent = make_run("pipe-a", now)
    tmp_store.append(recent)

    cutoff = now - timedelta(days=1)
    dest = archive_before(tmp_store, cutoff)

    archived = load_archive(dest)
    assert archived == []
    assert len(tmp_store.load_all()) == 1


def test_list_archives_returns_files(tmp_store: RunStore) -> None:
    now = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    run = make_run("pipe-a", now - timedelta(days=10))
    tmp_store.append(run)

    cutoff = now
    archive_before(tmp_store, cutoff, label="test-archive")

    store_path = Path(tmp_store.path)
    archives = list_archives(store_path)
    assert any("test-archive" in p.name for p in archives)


def test_list_archives_empty(tmp_path: Path) -> None:
    store_path = tmp_path / "runs" / "runs.jsonl"
    archives = list_archives(store_path)
    assert archives == []


def test_load_archive_preserves_fields(tmp_store: RunStore) -> None:
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    run = make_run("pipe-z", now - timedelta(days=3))
    run.error_message = "boom"
    tmp_store.append(run)

    dest = archive_before(tmp_store, now)
    archived = load_archive(dest)
    assert archived[0].pipeline_name == "pipe-z"
    assert archived[0].error_message == "boom"
