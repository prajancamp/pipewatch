"""Tests for pipewatch.cli_cluster."""
from __future__ import annotations
import argparse
import pytest
from datetime import datetime, timezone
from pipewatch.store import RunStore
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.cli_cluster import cmd_cluster


@pytest.fixture
def store_path(tmp_path):
    return tmp_path / "runs.jsonl"


def make_run(run_id, pipeline, status=PipelineStatus.FAILED, error=None):
    now = datetime.now(timezone.utc).isoformat()
    return PipelineRun(
        run_id=run_id,
        pipeline=pipeline,
        status=status,
        started_at=now,
        ended_at=now,
        error=error,
    )


def make_args(store_path, pipeline=None):
    ns = argparse.Namespace()
    ns.store = str(store_path)
    ns.pipeline = pipeline
    return ns


def test_cluster_empty_store(store_path, capsys):
    cmd_cluster(make_args(store_path))
    out = capsys.readouterr().out
    assert "No failure clusters found" in out


def test_cluster_shows_error_key(store_path, capsys):
    store = RunStore(str(store_path))
    store.append(make_run("r1", "etl", error="connection refused port 5432"))
    store.append(make_run("r2", "etl", error="connection refused port 5433"))
    cmd_cluster(make_args(store_path))
    out = capsys.readouterr().out
    assert "connection refused" in out


def test_cluster_filter_by_pipeline(store_path, capsys):
    store = RunStore(str(store_path))
    store.append(make_run("r1", "pipe_a", error="disk full"))
    store.append(make_run("r2", "pipe_b", error="oom killed"))
    cmd_cluster(make_args(store_path, pipeline="pipe_a"))
    out = capsys.readouterr().out
    assert "disk full" in out or "pipe_a" in out
    assert "oom" not in out


def test_cluster_success_runs_excluded(store_path, capsys):
    store = RunStore(str(store_path))
    store.append(make_run("r1", "pipe", status=PipelineStatus.SUCCESS))
    cmd_cluster(make_args(store_path))
    out = capsys.readouterr().out
    assert "No failure clusters found" in out
