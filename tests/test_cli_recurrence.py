"""Tests for pipewatch.cli_recurrence"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore
from pipewatch.cli_recurrence import cmd_recurrence


def make_run(
    pipeline: str = "pipe",
    status: PipelineStatus = PipelineStatus.SUCCESS,
    hour: int = 10,
    day: int = 1,
) -> PipelineRun:
    started = datetime(2024, 1, day, hour, 0, 0, tzinfo=timezone.utc).isoformat()
    ended = datetime(2024, 1, day, hour, 0, 30, tzinfo=timezone.utc).isoformat()
    return PipelineRun(
        run_id=f"{pipeline}-{day}-{hour}",
        pipeline=pipeline,
        status=status,
        started_at=started,
        ended_at=ended,
    )


@pytest.fixture()
def store_path(tmp_path: Path) -> Path:
    return tmp_path / "runs.jsonl"


def make_args(store_path: Path, pipeline=None, min_occurrences=3, min_failure_rate=0.5):
    return argparse.Namespace(
        store=store_path,
        pipeline=pipeline,
        min_occurrences=min_occurrences,
        min_failure_rate=min_failure_rate,
    )


def seed(store_path: Path, runs):
    s = RunStore(store_path)
    for r in runs:
        s.append(r)


def test_recurrence_empty_store(store_path, capsys):
    cmd_recurrence(make_args(store_path))
    out = capsys.readouterr().out
    assert "No runs" in out


def test_recurrence_no_flagged_slots(store_path, capsys):
    runs = [make_run(status=PipelineStatus.SUCCESS, hour=9, day=d) for d in range(1, 5)]
    seed(store_path, runs)
    cmd_recurrence(make_args(store_path))
    out = capsys.readouterr().out
    assert "No recurrent" in out


def test_recurrence_shows_flagged_slot(store_path, capsys):
    runs = [
        make_run(pipeline="etl", status=PipelineStatus.FAILED, hour=3, day=d)
        for d in range(1, 6)
    ]
    seed(store_path, runs)
    cmd_recurrence(make_args(store_path, min_occurrences=3))
    out = capsys.readouterr().out
    assert "etl" in out
    assert "3h" in out or "03" in out


def test_recurrence_filter_pipeline(store_path, capsys):
    runs_a = [
        make_run(pipeline="alpha", status=PipelineStatus.FAILED, hour=7, day=d)
        for d in range(1, 5)
    ]
    runs_b = [
        make_run(pipeline="beta", status=PipelineStatus.FAILED, hour=7, day=d)
        for d in range(1, 5)
    ]
    seed(store_path, runs_a + runs_b)
    cmd_recurrence(make_args(store_path, pipeline="alpha", min_occurrences=3))
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" not in out
