"""Tests for pipewatch.cli_pareto"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore
from pipewatch.cli_pareto import cmd_pareto


def make_run(
    pipeline: str,
    status: PipelineStatus = PipelineStatus.SUCCESS,
) -> PipelineRun:
    now = datetime.now(timezone.utc).isoformat()
    return PipelineRun(
        run_id=str(uuid.uuid4()),
        pipeline=pipeline,
        status=status,
        started_at=now,
        ended_at=now,
    )


@pytest.fixture()
def store_path(tmp_path: Path) -> Path:
    return tmp_path / "runs.jsonl"


def make_args(store_path: Path, pipeline=None, threshold=0.8):
    return SimpleNamespace(store=store_path, pipeline=pipeline, threshold=threshold)


def seed(store_path: Path, runs):
    s = RunStore(store_path)
    for r in runs:
        s.append(r)


def test_pareto_empty_store(store_path, capsys):
    cmd_pareto(make_args(store_path))
    out = capsys.readouterr().out
    assert "No pipeline runs found" in out


def test_pareto_all_success(store_path, capsys):
    seed(store_path, [make_run("pipe-a") for _ in range(4)])
    cmd_pareto(make_args(store_path))
    out = capsys.readouterr().out
    assert "pipe-a" in out
    assert "Total failures" in out


def test_pareto_shows_sorted_pipelines(store_path, capsys):
    runs = (
        [make_run("pipe-b", PipelineStatus.FAILED)] * 6
        + [make_run("pipe-a", PipelineStatus.FAILED)] * 2
    )
    seed(store_path, runs)
    cmd_pareto(make_args(store_path))
    out = capsys.readouterr().out
    assert out.index("pipe-b") < out.index("pipe-a")


def test_pareto_threshold_label(store_path, capsys):
    runs = (
        [make_run("pipe-a", PipelineStatus.FAILED)] * 8
        + [make_run("pipe-b", PipelineStatus.FAILED)] * 2
    )
    seed(store_path, runs)
    cmd_pareto(make_args(store_path, threshold=0.8))
    out = capsys.readouterr().out
    assert "<--" in out  # boundary marker present


def test_pareto_filter_pipeline(store_path, capsys):
    runs = (
        [make_run("pipe-a", PipelineStatus.FAILED)] * 3
        + [make_run("pipe-b", PipelineStatus.FAILED)] * 3
    )
    seed(store_path, runs)
    cmd_pareto(make_args(store_path, pipeline="pipe-a"))
    out = capsys.readouterr().out
    assert "pipe-a" in out
    assert "pipe-b" not in out
