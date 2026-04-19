"""Tests for pipewatch.cli_watchdog."""
import argparse
from datetime import datetime, timezone, timedelta
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore
from pipewatch.cli_watchdog import cmd_watchdog
import pytest


def make_run(pipeline, minutes_ago, run_id=None):
    started = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    finished = started + timedelta(seconds=5)
    return PipelineRun(
        run_id=run_id or f"{pipeline}-{minutes_ago}",
        pipeline=pipeline,
        status=PipelineStatus.SUCCESS,
        started_at=started,
        finished_at=finished,
    )


@pytest.fixture
def store_path(tmp_path):
    return str(tmp_path / "runs.jsonl")


def make_args(store, stale_after=60, threshold_override=None):
    return argparse.Namespace(
        store=store,
        stale_after=stale_after,
        threshold_override=threshold_override,
    )


def test_watchdog_empty_store(store_path, capsys):
    cmd_watchdog(make_args(store_path))
    out = capsys.readouterr().out
    assert "on time" in out


def test_watchdog_no_stale(store_path, capsys):
    s = RunStore(store_path)
    s.append(make_run("etl", 5))
    cmd_watchdog(make_args(store_path))
    out = capsys.readouterr().out
    assert "on time" in out


def test_watchdog_detects_stale(store_path, capsys):
    s = RunStore(store_path)
    s.append(make_run("etl", 120))
    cmd_watchdog(make_args(store_path, stale_after=60))
    out = capsys.readouterr().out
    assert "etl" in out


def test_watchdog_threshold_override(store_path, capsys):
    s = RunStore(store_path)
    s.append(make_run("fast", 20))
    cmd_watchdog(make_args(store_path, stale_after=60, threshold_override=["fast=10"]))
    out = capsys.readouterr().out
    assert "fast" in out


def test_watchdog_bad_override_ignored(store_path, capsys):
    s = RunStore(store_path)
    s.append(make_run("etl", 5))
    cmd_watchdog(make_args(store_path, threshold_override=["badvalue"]))
    out = capsys.readouterr().out
    assert "on time" in out
