"""Tests for pipewatch.cli_incident."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore
from pipewatch.cli_incident import cmd_incident


def make_run(
    pipeline: str = "pipe",
    status: str = "failed",
    offset_minutes: int = 0,
) -> PipelineRun:
    start = datetime(2024, 1, 1, 12, 0, 0) + timedelta(minutes=offset_minutes)
    end = start + timedelta(minutes=1)
    return PipelineRun(
        run_id=str(uuid.uuid4()),
        pipeline=pipeline,
        status=PipelineStatus(status),
        started_at=start.isoformat(),
        finished_at=end.isoformat(),
        error="err" if status == "failed" else None,
    )


@pytest.fixture()
def store_path(tmp_path: Path) -> Path:
    return tmp_path / "runs.jsonl"


def make_args(store_path: Path, pipeline=None, min_length=2, verbose=False):
    return SimpleNamespace(
        store=store_path,
        pipeline=pipeline,
        min_length=min_length,
        verbose=verbose,
    )


def seed(store_path: Path, runs):
    s = RunStore(store_path)
    for r in runs:
        s.append(r)


def test_incident_empty_store(store_path, capsys):
    cmd_incident(make_args(store_path))
    out = capsys.readouterr().out
    assert "No incidents" in out


def test_incident_detects_consecutive_failures(store_path, capsys):
    seed(store_path, [
        make_run(status="failed", offset_minutes=0),
        make_run(status="failed", offset_minutes=1),
    ])
    cmd_incident(make_args(store_path))
    out = capsys.readouterr().out
    assert "Incident" in out


def test_incident_filter_pipeline(store_path, capsys):
    seed(store_path, [
        make_run(pipeline="a", status="failed", offset_minutes=0),
        make_run(pipeline="a", status="failed", offset_minutes=1),
        make_run(pipeline="b", status="failed", offset_minutes=0),
        make_run(pipeline="b", status="failed", offset_minutes=1),
    ])
    cmd_incident(make_args(store_path, pipeline="a"))
    out = capsys.readouterr().out
    assert "'a'" in out
    assert "'b'" not in out


def test_incident_verbose_shows_run_ids(store_path, capsys):
    runs = [
        make_run(status="failed", offset_minutes=0),
        make_run(status="failed", offset_minutes=1),
    ]
    seed(store_path, runs)
    cmd_incident(make_args(store_path, verbose=True))
    out = capsys.readouterr().out
    assert runs[0].run_id in out
    assert runs[1].run_id in out
