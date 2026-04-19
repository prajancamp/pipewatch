"""Tests for pipewatch.snapshot."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.analyzer import PipelineStats
from pipewatch.snapshot import (
    Snapshot,
    capture_snapshot,
    diff_snapshots,
    load_snapshot,
    save_snapshot,
)


def make_stats(pid: str, success_rate: float = 1.0, avg_duration: float = 5.0) -> PipelineStats:
    return PipelineStats(
        pipeline_id=pid,
        total_runs=10,
        success_count=int(10 * success_rate),
        failure_count=int(10 * (1 - success_rate)),
        success_rate=success_rate,
        avg_duration=avg_duration,
        max_consecutive_failures=0,
    )


def test_capture_snapshot_keys():
    stats = [make_stats("etl.a"), make_stats("etl.b")]
    snap = capture_snapshot(stats)
    assert "etl.a" in snap.pipelines
    assert "etl.b" in snap.pipelines
    assert snap.captured_at


def test_snapshot_roundtrip():
    stats = [make_stats("etl.x", success_rate=0.8, avg_duration=12.5)]
    snap = capture_snapshot(stats)
    restored = Snapshot.from_dict(snap.to_dict())
    assert restored.captured_at == snap.captured_at
    assert set(restored.pipelines.keys()) == {"etl.x"}


def test_save_and_load_snapshot(tmp_path: Path):
    p = tmp_path / "snaps" / "snap.json"
    stats = [make_stats("etl.y")]
    snap = capture_snapshot(stats)
    save_snapshot(snap, p)
    assert p.exists()
    loaded = load_snapshot(p)
    assert loaded is not None
    assert loaded.captured_at == snap.captured_at


def test_load_missing_snapshot(tmp_path: Path):
    result = load_snapshot(tmp_path / "no_such_file.json")
    assert result is None


def test_diff_snapshots_detects_change():
    old = capture_snapshot([make_stats("etl.a", success_rate=0.9, avg_duration=10.0)])
    new = capture_snapshot([make_stats("etl.a", success_rate=0.7, avg_duration=15.0)])
    diff = diff_snapshots(old, new)
    assert "etl.a" in diff
    assert abs(diff["etl.a"]["success_rate"] - (-0.2)) < 0.001
    assert abs(diff["etl.a"]["avg_duration"] - 5.0) < 0.001


def test_diff_snapshots_new_pipeline():
    old = capture_snapshot([make_stats("etl.a")])
    new = capture_snapshot([make_stats("etl.a"), make_stats("etl.b")])
    diff = diff_snapshots(old, new)
    assert diff.get("etl.b") == {"new": True}
