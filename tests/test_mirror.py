"""Tests for pipewatch.mirror."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest

from pipewatch.mirror import MirrorEntry, compute_mirror
from pipewatch.models import PipelineRun, PipelineStatus


def make_run(
    pipeline: str,
    status: PipelineStatus = PipelineStatus.SUCCESS,
    started: str = "2024-01-01T10:00:00",
    duration: Optional[float] = 60.0,
) -> PipelineRun:
    started_dt = datetime.fromisoformat(started).replace(tzinfo=timezone.utc)
    return PipelineRun(
        run_id=f"{pipeline}-{started}",
        pipeline=pipeline,
        status=status,
        started_at=started_dt,
        duration=duration,
    )


def test_compute_mirror_both_sides():
    left = [make_run("etl", PipelineStatus.SUCCESS, "2024-01-01T10:00:00")]
    right = [make_run("etl", PipelineStatus.FAILURE, "2024-01-02T10:00:00")]
    entries = compute_mirror(left, right)
    assert len(entries) == 1
    e = entries[0]
    assert e.pipeline == "etl"
    assert e.left_success_rate == pytest.approx(1.0)
    assert e.right_success_rate == pytest.approx(0.0)
    assert e.success_rate_delta == pytest.approx(-1.0)


def test_compute_mirror_pipeline_only_in_left():
    left = [make_run("alpha")]
    right = []
    entries = compute_mirror(left, right)
    assert len(entries) == 1
    e = entries[0]
    assert e.right_total == 0
    assert e.right_success_rate is None
    assert e.success_rate_delta is None


def test_compute_mirror_pipeline_only_in_right():
    left = []
    right = [make_run("beta")]
    entries = compute_mirror(left, right)
    assert len(entries) == 1
    e = entries[0]
    assert e.left_total == 0
    assert e.left_success_rate is None


def test_compute_mirror_filter_pipeline():
    left = [make_run("a"), make_run("b")]
    right = [make_run("a"), make_run("b")]
    entries = compute_mirror(left, right, pipeline="a")
    assert len(entries) == 1
    assert entries[0].pipeline == "a"


def test_compute_mirror_empty():
    entries = compute_mirror([], [])
    assert entries == []


def test_compute_mirror_duration_delta():
    left = [make_run("etl", duration=30.0)]
    right = [make_run("etl", duration=90.0)]
    entries = compute_mirror(left, right)
    assert entries[0].duration_delta == pytest.approx(60.0)


def test_mirror_entry_str():
    e = MirrorEntry(
        pipeline="pipe",
        left_success_rate=0.8,
        right_success_rate=0.6,
        left_total=10,
        right_total=10,
        left_avg_duration=None,
        right_avg_duration=None,
    )
    s = str(e)
    assert "pipe" in s
    assert "-20.0%" in s


def test_compute_mirror_sorted_pipelines():
    left = [make_run("z-pipe"), make_run("a-pipe")]
    right = [make_run("z-pipe"), make_run("a-pipe")]
    entries = compute_mirror(left, right)
    names = [e.pipeline for e in entries]
    assert names == sorted(names)
