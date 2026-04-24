"""Tests for pipewatch.window sliding window aggregation."""
from datetime import datetime, timedelta
from typing import Optional

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.window import WindowSummary, compute_window


NOW = datetime(2024, 6, 1, 12, 0, 0)


def make_run(
    pipeline: str,
    status: PipelineStatus,
    minutes_ago: float,
    duration: Optional[float] = None,
) -> PipelineRun:
    started = NOW - timedelta(minutes=minutes_ago)
    ended = started + timedelta(seconds=duration) if duration else None
    return PipelineRun(
        run_id=f"{pipeline}-{minutes_ago}",
        pipeline=pipeline,
        status=status,
        started_at=started,
        ended_at=ended,
    )


def test_compute_window_empty():
    result = compute_window([], window_minutes=60, reference_time=NOW)
    assert result == []


def test_compute_window_all_within():
    runs = [
        make_run("etl", PipelineStatus.SUCCESS, 10, duration=30.0),
        make_run("etl", PipelineStatus.FAILED, 20, duration=5.0),
        make_run("etl", PipelineStatus.SUCCESS, 30, duration=20.0),
    ]
    result = compute_window(runs, window_minutes=60, reference_time=NOW)
    assert len(result) == 1
    s = result[0]
    assert s.pipeline == "etl"
    assert s.total == 3
    assert s.failures == 1
    assert s.successes == 2
    assert s.failure_rate == pytest.approx(1 / 3)


def test_compute_window_excludes_old_runs():
    runs = [
        make_run("etl", PipelineStatus.FAILED, 90),   # outside 60-min window
        make_run("etl", PipelineStatus.SUCCESS, 10),  # inside
    ]
    result = compute_window(runs, window_minutes=60, reference_time=NOW)
    assert len(result) == 1
    assert result[0].total == 1
    assert result[0].failures == 0


def test_compute_window_avg_duration():
    runs = [
        make_run("pipe", PipelineStatus.SUCCESS, 5, duration=100.0),
        make_run("pipe", PipelineStatus.SUCCESS, 10, duration=200.0),
    ]
    result = compute_window(runs, window_minutes=60, reference_time=NOW)
    assert result[0].avg_duration == pytest.approx(150.0)


def test_compute_window_no_duration():
    runs = [make_run("pipe", PipelineStatus.SUCCESS, 5)]
    result = compute_window(runs, window_minutes=60, reference_time=NOW)
    assert result[0].avg_duration is None


def test_compute_window_filter_pipeline():
    runs = [
        make_run("a", PipelineStatus.SUCCESS, 5),
        make_run("b", PipelineStatus.FAILED, 10),
    ]
    result = compute_window(runs, window_minutes=60, pipeline="a", reference_time=NOW)
    assert len(result) == 1
    assert result[0].pipeline == "a"


def test_compute_window_multiple_pipelines():
    runs = [
        make_run("alpha", PipelineStatus.SUCCESS, 5),
        make_run("beta", PipelineStatus.FAILED, 5),
        make_run("alpha", PipelineStatus.FAILED, 15),
    ]
    result = compute_window(runs, window_minutes=60, reference_time=NOW)
    assert len(result) == 2
    names = [s.pipeline for s in result]
    assert "alpha" in names and "beta" in names


def test_window_summary_str():
    s = WindowSummary(
        pipeline="my_pipe",
        window_minutes=30,
        total=4,
        failures=1,
        successes=3,
        avg_duration=42.5,
    )
    text = str(s)
    assert "my_pipe" in text
    assert "30m" in text
    assert "25.0%" in text
    assert "42.5s" in text


def test_failure_rate_zero_total():
    s = WindowSummary(
        pipeline="p", window_minutes=60,
        total=0, failures=0, successes=0, avg_duration=None,
    )
    assert s.failure_rate == 0.0
