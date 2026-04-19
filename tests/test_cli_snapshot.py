"""Tests for pipewatch.cli_snapshot commands."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore
from pipewatch.snapshot import save_snapshot, capture_snapshot
from pipewatch.analyzer import PipelineStats
from pipewatch.cli_snapshot import cmd_snapshot_save, cmd_snapshot_diff


def make_args(**kwargs) -> argparse.Namespace:
    return argparse.Namespace(**kwargs)


def seed_store(path: Path) -> None:
    store = RunStore(path)
    for i in range(3):
        store.append(
            PipelineRun(
                run_id=f"r{i}",
                pipeline_id="etl.test",
                status=PipelineStatus.SUCCESS,
                started_at="2024-01-01T00:00:00",
                ended_at="2024-01-01T00:01:00",
            )
        )


def test_snapshot_save_creates_file(tmp_path: Path, capsys):
    store_path = tmp_path / "runs.jsonl"
    seed_store(store_path)
    out_path = tmp_path / "snap.json"
    args = make_args(store=str(store_path), output=str(out_path))
    cmd_snapshot_save(args)
    assert out_path.exists()
    data = json.loads(out_path.read_text())
    assert "captured_at" in data
    assert "etl.test" in data["pipelines"]
    captured = capsys.readouterr()
    assert "Snapshot saved" in captured.out


def test_snapshot_diff_detects_new(tmp_path: Path, capsys):
    def make_snap(pid, rate, dur):
        s = PipelineStats(
            pipeline_id=pid, total_runs=5, success_count=int(5*rate),
            failure_count=int(5*(1-rate)), success_rate=rate,
            avg_duration=dur, max_consecutive_failures=0,
        )
        return capture_snapshot([s])

    old_path = tmp_path / "old.json"
    new_path = tmp_path / "new.json"
    save_snapshot(make_snap("etl.a", 1.0, 5.0), old_path)
    save_snapshot(make_snap("etl.b", 0.5, 8.0), new_path)

    args = make_args(old=str(old_path), new=str(new_path))
    cmd_snapshot_diff(args)
    out = capsys.readouterr().out
    assert "etl.b" in out
    assert "NEW" in out


def test_snapshot_diff_missing_old(tmp_path: Path, capsys):
    new_path = tmp_path / "new.json"
    save_snapshot(capture_snapshot([]), new_path)
    args = make_args(old=str(tmp_path / "nope.json"), new=str(new_path))
    cmd_snapshot_diff(args)
    assert "ERROR" in capsys.readouterr().out


def test_snapshot_diff_no_changes(tmp_path: Path, capsys):
    from pipewatch.snapshot import capture_snapshot, save_snapshot
    snap = capture_snapshot([])
    p1, p2 = tmp_path / "a.json", tmp_path / "b.json"
    save_snapshot(snap, p1)
    save_snapshot(snap, p2)
    args = make_args(old=str(p1), new=str(p2))
    cmd_snapshot_diff(args)
    assert "No changes" in capsys.readouterr().out
