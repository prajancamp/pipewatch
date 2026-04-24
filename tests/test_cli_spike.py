"""Tests for pipewatch.cli_spike — CLI integration for spike detection."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.cli_spike import cmd_spike
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore


def make_run(
    pipeline: str,
    status: PipelineStatus,
    minutes_ago: float,
    store: RunStore,
) -> None:
    now = datetime.now(tz=timezone.utc)
    started = now - timedelta(minutes=minutes_ago)
    ended = started + timedelta(seconds=15)
    run = PipelineRun(
        run_id=f"{pipeline}-{minutes_ago}",
        pipeline=pipeline,
        status=status,
        started_at=started,
        ended_at=ended,
    )
    store.append(run)


@pytest.fixture()
def store_path(tmp_path: Path) -> Path:
    return tmp_path / "runs.jsonl"


def make_args(store_path: Path, **kwargs) -> argparse.Namespace:
    defaults = dict(
        store=str(store_path),
        window=30,
        lookback=360,
        multiplier=2.0,
        min_count=2,
        pipeline=None,
        verbose=False,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_spike_empty_store(store_path: Path, capsys):
    args = make_args(store_path)
    cmd_spike(args)
    out = capsys.readouterr().out
    assert "No pipeline activity" in out


def test_spike_no_flagged_healthy_pipeline(store_path: Path, capsys):
    store = RunStore(str(store_path))
    for i in range(5):
        make_run("healthy", PipelineStatus.SUCCESS, i + 1, store)
    args = make_args(store_path)
    cmd_spike(args)
    out = capsys.readouterr().out
    assert "No spikes detected" in out


def test_spike_detects_burst(store_path: Path, capsys):
    store = RunStore(str(store_path))
    # Healthy baseline
    for i in range(8):
        make_run("pipe", PipelineStatus.SUCCESS, 60 + i, store)
    make_run("pipe", PipelineStatus.FAILED, 70, store)
    # Spike in recent window
    for i in range(4):
        make_run("pipe", PipelineStatus.FAILED, i + 1, store)
    args = make_args(store_path, min_count=2, multiplier=2.0)
    cmd_spike(args)
    out = capsys.readouterr().out
    assert "SPIKE" in out or "pipe" in out


def test_spike_verbose_shows_ok_pipelines(store_path: Path, capsys):
    store = RunStore(str(store_path))
    for i in range(5):
        make_run("stable", PipelineStatus.SUCCESS, i + 1, store)
    args = make_args(store_path, verbose=True)
    cmd_spike(args)
    out = capsys.readouterr().out
    assert "stable" in out
