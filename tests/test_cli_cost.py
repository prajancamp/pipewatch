"""Tests for pipewatch.cli_cost module."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore
from pipewatch.cli_cost import cmd_cost


def make_run(
    pipeline: str = "etl",
    duration: float | None = 120.0,
) -> PipelineRun:
    now = datetime.now(timezone.utc).isoformat()
    return PipelineRun(
        run_id=str(uuid.uuid4()),
        pipeline=pipeline,
        status=PipelineStatus.SUCCESS,
        started_at=now,
        ended_at=now,
        duration=duration,
    )


@pytest.fixture()
def store_path(tmp_path: Path) -> Path:
    return tmp_path / "runs.jsonl"


def make_args(store_path: Path, pipeline: str | None = None, rate: float = 0.0001):
    class Args:
        store = str(store_path)
        pass
    a = Args()
    a.pipeline = pipeline
    a.rate = rate
    return a


def test_cost_empty_store(store_path: Path, capsys: pytest.CaptureFixture) -> None:
    cmd_cost(make_args(store_path))
    out = capsys.readouterr().out
    assert "No runs found" in out


def test_cost_shows_summary(store_path: Path, capsys: pytest.CaptureFixture) -> None:
    store = RunStore(str(store_path))
    store.append(make_run(pipeline="etl", duration=100.0))
    store.append(make_run(pipeline="etl", duration=200.0))
    cmd_cost(make_args(store_path, rate=0.01))
    out = capsys.readouterr().out
    assert "etl" in out
    assert "runs=2" in out


def test_cost_filter_pipeline(store_path: Path, capsys: pytest.CaptureFixture) -> None:
    store = RunStore(str(store_path))
    store.append(make_run(pipeline="etl", duration=100.0))
    store.append(make_run(pipeline="loader", duration=50.0))
    cmd_cost(make_args(store_path, pipeline="etl"))
    out = capsys.readouterr().out
    assert "etl" in out
    assert "loader" not in out


def test_cost_grand_total(store_path: Path, capsys: pytest.CaptureFixture) -> None:
    store = RunStore(str(store_path))
    store.append(make_run(duration=10000.0))
    cmd_cost(make_args(store_path, rate=0.0001))
    out = capsys.readouterr().out
    assert "Grand total" in out
    assert "$1.0000" in out
