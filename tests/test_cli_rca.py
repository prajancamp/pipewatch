"""Tests for pipewatch.cli_rca — CLI integration for root cause analysis."""
from __future__ import annotations

import uuid
import argparse
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore
from pipewatch.cli_rca import cmd_rca


def make_run(
    pipeline: str = "pipe_a",
    status: PipelineStatus = PipelineStatus.FAILED,
    error: str | None = "timeout error",
) -> PipelineRun:
    now = datetime.now(timezone.utc).isoformat()
    return PipelineRun(
        run_id=str(uuid.uuid4()),
        pipeline=pipeline,
        status=status,
        started_at=now,
        ended_at=now,
        error=error,
    )


@pytest.fixture()
def store_path(tmp_path: Path) -> Path:
    return tmp_path / "runs.jsonl"


def make_args(store_path: Path, pipeline=None, limit=20, verbose=False):
    ns = argparse.Namespace(
        store=str(store_path),
        pipeline=pipeline,
        limit=limit,
        verbose=verbose,
    )
    return ns


def seed(store_path: Path, *runs: PipelineRun) -> None:
    s = RunStore(str(store_path))
    for r in runs:
        s.append(r)


def test_rca_empty_store(store_path, capsys):
    cmd_rca(make_args(store_path))
    out = capsys.readouterr().out
    assert "No failed runs" in out


def test_rca_shows_finding(store_path, capsys):
    seed(store_path, make_run(error="permission denied"))
    cmd_rca(make_args(store_path))
    out = capsys.readouterr().out
    assert "pipe_a" in out


def test_rca_filter_pipeline(store_path, capsys):
    seed(
        store_path,
        make_run(pipeline="alpha", error="timeout"),
        make_run(pipeline="beta", error="schema mismatch"),
    )
    cmd_rca(make_args(store_path, pipeline="alpha"))
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" not in out


def test_rca_verbose_shows_error(store_path, capsys):
    seed(store_path, make_run(error="disk full on /var"))
    cmd_rca(make_args(store_path, verbose=True))
    out = capsys.readouterr().out
    assert "disk full on /var" in out


def test_rca_limit_respected(store_path, capsys):
    for _ in range(10):
        seed(store_path, make_run(error="null pointer"))
    cmd_rca(make_args(store_path, limit=3))
    out = capsys.readouterr().out
    # Each finding prints one line with the pipeline name
    lines = [l for l in out.splitlines() if "pipe_a" in l]
    assert len(lines) <= 3
