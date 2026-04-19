"""Tests for pipewatch.cli_archive."""

from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.archive import archive_before
from pipewatch.cli_archive import cmd_archive, cmd_archive_list, cmd_archive_inspect
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore


def make_run(name: str, started_at: datetime) -> PipelineRun:
    return PipelineRun(
        run_id=str(uuid.uuid4()),
        pipeline_name=name,
        status=PipelineStatus.SUCCESS,
        started_at=started_at,
        ended_at=started_at + timedelta(seconds=5),
    )


def make_args(store: str, **kwargs) -> argparse.Namespace:
    return argparse.Namespace(store=store, **kwargs)


@pytest.fixture()
def store_path(tmp_path: Path) -> str:
    p = tmp_path / "runs" / "runs.jsonl"
    p.parent.mkdir(parents=True)
    return str(p)


def test_cmd_archive_creates_file(store_path: str, capsys) -> None:
    store = RunStore(store_path)
    now = datetime(2024, 3, 1, tzinfo=timezone.utc)
    store.append(make_run("pipe-a", now - timedelta(days=10)))

    args = make_args(store_path, before="2024-02-20T00:00:00", label="test")
    cmd_archive(args)
    out = capsys.readouterr().out
    assert "Archived" in out
    assert "test" in out


def test_cmd_archive_list_shows_archives(store_path: str, capsys) -> None:
    store = RunStore(store_path)
    now = datetime(2024, 3, 1, tzinfo=timezone.utc)
    store.append(make_run("pipe-b", now - timedelta(days=5)))
    archive_before(store, now, label="my-archive")

    args = make_args(store_path)
    cmd_archive_list(args)
    out = capsys.readouterr().out
    assert "my-archive" in out


def test_cmd_archive_list_empty(store_path: str, capsys) -> None:
    args = make_args(store_path)
    cmd_archive_list(args)
    out = capsys.readouterr().out
    assert "No archives" in out


def test_cmd_archive_inspect(store_path: str, capsys) -> None:
    store = RunStore(store_path)
    now = datetime(2024, 3, 1, tzinfo=timezone.utc)
    store.append(make_run("pipe-c", now - timedelta(days=2)))
    dest = archive_before(store, now, label="inspect-test")

    args = make_args(store_path, file=str(dest))
    cmd_archive_inspect(args)
    out = capsys.readouterr().out
    assert "pipe-c" in out
