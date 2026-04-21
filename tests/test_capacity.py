"""Tests for pipewatch.capacity."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.capacity import estimate_capacity, estimate_all_capacity


def make_run(
    pipeline: str,
    status: PipelineStatus = PipelineStatus.SUCCESS,
    minutes_ago: int = 30,
    duration: Optional[float] = 120.0,
) -> PipelineRun:
    now = datetime.now(timezone.utc)
    started = now - timedelta(minutes=minutes_ago)
    finished = started + timedelta(seconds=duration) if duration is not None else None
    return PipelineRun(
        run_id=f"{pipeline}-{minutes_ago}",
        pipeline=pipeline,
        status=status,
        started_at=started,
        finished_at=finished,
    )


def test_estimate_capacity_no_runs():
    result = estimate_capacity([], "etl", window_hours=24)
    assert result is None


def test_estimate_capacity_outside_window():
    run = make_run("etl", minutes_ago=200)  # outside 2-hour window
    result = estimate_capacity([run], "etl", window_hours=2)
    assert result is None


def test_estimate_capacity_basic():
    runs = [make_run("etl", minutes_ago=i * 10, duration=60.0) for i in range(1, 7)]
    result = estimate_capacity(runs, "etl", window_hours=24)
    assert result is not None
    assert result.pipeline == "etl"
    assert result.run_count == 6
    assert result.avg_duration_seconds == pytest.approx(60.0)
    # 6 runs in 24h → 6 runs/day projected
    assert result.projected_runs_per_day == pytest.approx(6.0)
    # 6 runs/day * 60s / 60 = 6 compute-min/day
    assert result.projected_compute_minutes_per_day == pytest.approx(6.0)


def test_estimate_capacity_no_duration():
    runs = [make_run("etl", minutes_ago=i * 10, duration=None) for i in range(1, 4)]
    result = estimate_capacity(runs, "etl", window_hours=24)
    assert result is not None
    assert result.avg_duration_seconds is None
    assert result.projected_compute_minutes_per_day == 0.0


def test_estimate_capacity_high_volume_note():
    # 150 runs in 24h window → HIGH
    runs = [make_run("etl", minutes_ago=i, duration=10.0) for i in range(1, 151)]
    result = estimate_capacity(runs, "etl", window_hours=24)
    assert result is not None
    assert "HIGH" in result.note


def test_estimate_capacity_low_volume_note():
    runs = [make_run("etl", minutes_ago=i * 60, duration=10.0) for i in range(1, 4)]
    result = estimate_capacity(runs, "etl", window_hours=24)
    assert result is not None
    assert "LOW" in result.note


def test_estimate_all_capacity_multiple_pipelines():
    runs = [
        make_run("alpha", minutes_ago=10, duration=30.0),
        make_run("alpha", minutes_ago=20, duration=30.0),
        make_run("beta", minutes_ago=15, duration=90.0),
    ]
    results = estimate_all_capacity(runs, window_hours=24)
    names = [r.pipeline for r in results]
    assert "alpha" in names
    assert "beta" in names


def test_estimate_all_capacity_empty():
    results = estimate_all_capacity([], window_hours=24)
    assert results == []


def test_str_representation():
    runs = [make_run("pipe", minutes_ago=5, duration=120.0)]
    result = estimate_capacity(runs, "pipe", window_hours=24)
    assert result is not None
    text = str(result)
    assert "pipe" in text
    assert "runs/day" in text
