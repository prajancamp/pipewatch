"""Tests for pipewatch.checkpoint."""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from pipewatch.checkpoint import (
    CheckpointEntry,
    get_checkpoint,
    load_checkpoints,
    save_checkpoints,
    seconds_since_checkpoint,
    update_checkpoints,
)
from pipewatch.models import PipelineRun, PipelineStatus


def make_run(pipeline: str, status: PipelineStatus, started_at: str, duration: float = 10.0) -> PipelineRun:
    return PipelineRun(
        run_id=f"run-{pipeline}-{started_at}",
        pipeline=pipeline,
        status=status,
        started_at=started_at,
        ended_at=started_at,
        duration=duration,
    )


@pytest.fixture
def store_path(tmp_path):
    return str(tmp_path / "runs.jsonl")


def test_load_checkpoints_missing(store_path):
    result = load_checkpoints(store_path)
    assert result == {}


def test_save_and_load_roundtrip(store_path):
    entries = {
        "etl": CheckpointEntry(pipeline="etl", last_success="2024-01-01T10:00:00", run_id="r1", duration=5.0)
    }
    save_checkpoints(store_path, entries)
    loaded = load_checkpoints(store_path)
    assert "etl" in loaded
    assert loaded["etl"].run_id == "r1"
    assert loaded["etl"].duration == 5.0


def test_update_checkpoints_only_success(store_path):
    runs = [
        make_run("etl", PipelineStatus.SUCCESS, "2024-01-01T09:00:00"),
        make_run("etl", PipelineStatus.FAILED, "2024-01-01T10:00:00"),
    ]
    checkpoints = update_checkpoints(store_path, runs)
    assert checkpoints["etl"].last_success == "2024-01-01T09:00:00"


def test_update_checkpoints_advances_to_latest_success(store_path):
    runs = [
        make_run("etl", PipelineStatus.SUCCESS, "2024-01-01T08:00:00"),
        make_run("etl", PipelineStatus.SUCCESS, "2024-01-01T12:00:00"),
    ]
    checkpoints = update_checkpoints(store_path, runs)
    assert checkpoints["etl"].last_success == "2024-01-01T12:00:00"


def test_update_checkpoints_multiple_pipelines(store_path):
    runs = [
        make_run("etl", PipelineStatus.SUCCESS, "2024-01-01T08:00:00"),
        make_run("load", PipelineStatus.SUCCESS, "2024-01-01T09:00:00"),
    ]
    checkpoints = update_checkpoints(store_path, runs)
    assert "etl" in checkpoints
    assert "load" in checkpoints


def test_get_checkpoint_returns_none_for_unknown(store_path):
    assert get_checkpoint(store_path, "missing") is None


def test_get_checkpoint_returns_entry(store_path):
    runs = [make_run("etl", PipelineStatus.SUCCESS, "2024-06-01T10:00:00", duration=30.0)]
    update_checkpoints(store_path, runs)
    entry = get_checkpoint(store_path, "etl")
    assert entry is not None
    assert entry.pipeline == "etl"
    assert entry.duration == 30.0


def test_seconds_since_checkpoint():
    past = (datetime.utcnow() - timedelta(seconds=120)).isoformat()
    entry = CheckpointEntry(pipeline="etl", last_success=past, run_id="r1", duration=None)
    age = seconds_since_checkpoint(entry)
    assert 110 < age < 130


def test_checkpoint_str_no_duration():
    entry = CheckpointEntry(pipeline="etl", last_success="2024-01-01T10:00:00", run_id="r1", duration=None)
    assert "n/a" in str(entry)
    assert "etl" in str(entry)
