"""Tests for pipewatch.cli_checkpoint."""
from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from pipewatch.cli_checkpoint import cmd_checkpoint_show, cmd_checkpoint_update
from pipewatch.checkpoint import update_checkpoints
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore


def make_run(pipeline, status, started_at):
    return PipelineRun(
        run_id=f"{pipeline}-{started_at}",
        pipeline=pipeline,
        status=status,
        started_at=started_at,
        ended_at=started_at,
        duration=5.0,
    )


@pytest.fixture
def store_path(tmp_path):
    return str(tmp_path / "runs.jsonl")


def make_args(store, **kwargs):
    ns = argparse.Namespace(store=store, **kwargs)
    return ns


def seed(store_path):
    store = RunStore(store_path)
    store.append(make_run("etl", PipelineStatus.SUCCESS, "2024-06-01T10:00:00"))
    store.append(make_run("etl", PipelineStatus.FAILED, "2024-06-01T11:00:00"))
    store.append(make_run("load", PipelineStatus.SUCCESS, "2024-06-01T09:00:00"))


def test_checkpoint_update_empty_store(store_path, capsys):
    args = make_args(store_path)
    cmd_checkpoint_update(args)
    out = capsys.readouterr().out
    assert "No successful" in out


def test_checkpoint_update_creates_entries(store_path, capsys):
    seed(store_path)
    args = make_args(store_path)
    cmd_checkpoint_update(args)
    out = capsys.readouterr().out
    assert "etl" in out
    assert "load" in out


def test_checkpoint_show_no_checkpoints(store_path, capsys):
    args = make_args(store_path, pipeline=None)
    cmd_checkpoint_show(args)
    out = capsys.readouterr().out
    assert "No checkpoints" in out


def test_checkpoint_show_specific_pipeline(store_path, capsys):
    seed(store_path)
    update_checkpoints(store_path, RunStore(store_path).load_all())
    args = make_args(store_path, pipeline="etl")
    cmd_checkpoint_show(args)
    out = capsys.readouterr().out
    assert "etl" in out
    assert "Age" in out


def test_checkpoint_show_missing_pipeline(store_path, capsys):
    seed(store_path)
    update_checkpoints(store_path, RunStore(store_path).load_all())
    args = make_args(store_path, pipeline="nonexistent")
    cmd_checkpoint_show(args)
    out = capsys.readouterr().out
    assert "No checkpoint found" in out
