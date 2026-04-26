"""Tests for pipewatch.cli_trace module."""
from __future__ import annotations

import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore
from pipewatch.cli_trace import cmd_trace


@pytest.fixture()
def store_path(tmp_path: Path) -> Path:
    return tmp_path / "runs.jsonl"


def make_args(store_path: Path, failed_only: bool = False, verbose: bool = False):
    return SimpleNamespace(store=store_path, failed_only=failed_only, verbose=verbose)


def make_run(
    pipeline: str = "pipe",
    status: PipelineStatus = PipelineStatus.SUCCESS,
    trace_id: Optional[str] = None,
    error: Optional[str] = None,
) -> PipelineRun:
    meta = {"trace_id": trace_id} if trace_id else {}
    return PipelineRun(
        run_id=str(uuid.uuid4()),
        pipeline=pipeline,
        status=status,
        started_at="2024-01-01T09:00:00",
        finished_at="2024-01-01T09:00:10",
        duration=10.0,
        error=error,
        tags=[],
        meta=meta,
    )


def seed(store_path: Path, runs) -> None:
    s = RunStore(store_path)
    for r in runs:
        s.append(r)


def test_trace_empty_store(store_path, capsys):
    cmd_trace(make_args(store_path))
    out = capsys.readouterr().out
    assert "No runs" in out


def test_trace_no_trace_metadata(store_path, capsys):
    seed(store_path, [make_run(pipeline="alpha")])
    cmd_trace(make_args(store_path))
    out = capsys.readouterr().out
    assert "No runs contain trace_id" in out


def test_trace_shows_summary(store_path, capsys):
    seed(store_path, [
        make_run(pipeline="a", trace_id="t1"),
        make_run(pipeline="b", trace_id="t1"),
    ])
    cmd_trace(make_args(store_path))
    out = capsys.readouterr().out
    assert "1 total" in out
    assert "t1" in out


def test_trace_failed_only_flag(store_path, capsys):
    seed(store_path, [
        make_run(pipeline="a", trace_id="t1", status=PipelineStatus.SUCCESS),
        make_run(pipeline="b", trace_id="t2", status=PipelineStatus.FAILED, error="boom"),
    ])
    cmd_trace(make_args(store_path, failed_only=True))
    out = capsys.readouterr().out
    assert "t2" in out
    assert "t1" not in out


def test_trace_verbose_shows_run_details(store_path, capsys):
    seed(store_path, [
        make_run(pipeline="alpha", trace_id="t1", status=PipelineStatus.FAILED, error="oops"),
    ])
    cmd_trace(make_args(store_path, verbose=True))
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "oops" in out
