"""Tests for pipewatch.cli_triage."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore
from pipewatch.cli_triage import cmd_triage


def make_run(
    pipeline: str,
    status: PipelineStatus,
    offset_hours: int = 0,
) -> PipelineRun:
    started = datetime(2024, 6, 1, tzinfo=timezone.utc) + timedelta(hours=offset_hours)
    return PipelineRun(
        run_id=f"{pipeline}-{offset_hours}",
        pipeline=pipeline,
        status=status,
        started_at=started,
        duration=5.0,
    )


@pytest.fixture()
def store_path(tmp_path: Path) -> Path:
    return tmp_path / "runs.jsonl"


def make_args(store_path: Path, min_priority: int = 1, pipeline=None) -> argparse.Namespace:
    return argparse.Namespace(
        store=store_path,
        min_priority=min_priority,
        pipeline=pipeline,
    )


def seed(store_path: Path, runs):
    s = RunStore(store_path)
    for r in runs:
        s.append(r)


def test_triage_empty_store(store_path, capsys):
    cmd_triage(make_args(store_path))
    out = capsys.readouterr().out
    assert "No pipeline runs found" in out


def test_triage_no_issues_at_threshold(store_path, capsys):
    runs = [make_run("healthy", PipelineStatus.SUCCESS, i) for i in range(5)]
    seed(store_path, runs)
    cmd_triage(make_args(store_path, min_priority=1))
    out = capsys.readouterr().out
    assert "Nothing requires attention" in out


def test_triage_shows_critical_pipeline(store_path, capsys):
    runs = [make_run("broken", PipelineStatus.FAILED, i) for i in range(6)]
    seed(store_path, runs)
    cmd_triage(make_args(store_path, min_priority=0))
    out = capsys.readouterr().out
    assert "broken" in out
    assert "CRITICAL" in out


def test_triage_filter_by_pipeline(store_path, capsys):
    runs = (
        [make_run("alpha", PipelineStatus.FAILED, i) for i in range(6)]
        + [make_run("beta", PipelineStatus.FAILED, i) for i in range(6)]
    )
    seed(store_path, runs)
    cmd_triage(make_args(store_path, min_priority=0, pipeline="alpha"))
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" not in out
