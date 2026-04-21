"""Tests for pipewatch.momentum."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.momentum import compute_momentum, compute_all_momentums


def make_run(pipeline: str, started_at: datetime, status: PipelineStatus = PipelineStatus.SUCCESS) -> PipelineRun:
    return PipelineRun(
        run_id=str(uuid4()),
        pipeline=pipeline,
        status=status,
        started_at=started_at,
        ended_at=started_at + timedelta(seconds=30),
    )


NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_compute_momentum_accelerating():
    runs = [
        make_run("etl", NOW - timedelta(hours=2)),
        make_run("etl", NOW - timedelta(hours=4)),
        make_run("etl", NOW - timedelta(hours=6)),
        make_run("etl", NOW - timedelta(hours=30)),  # prior window
    ]
    result = compute_momentum(runs, "etl", window_hours=24.0, now=NOW)
    assert result.trend == "accelerating"
    assert result.recent_run_count == 3
    assert result.prior_run_count == 1
    assert result.delta == 2


def test_compute_momentum_decelerating():
    runs = [
        make_run("etl", NOW - timedelta(hours=2)),
        make_run("etl", NOW - timedelta(hours=26)),
        make_run("etl", NOW - timedelta(hours=30)),
        make_run("etl", NOW - timedelta(hours=40)),
    ]
    result = compute_momentum(runs, "etl", window_hours=24.0, now=NOW)
    assert result.trend == "decelerating"
    assert result.recent_run_count == 1
    assert result.prior_run_count == 3
    assert result.delta == -2


def test_compute_momentum_stable():
    runs = [
        make_run("etl", NOW - timedelta(hours=6)),
        make_run("etl", NOW - timedelta(hours=18)),
        make_run("etl", NOW - timedelta(hours=30)),
        make_run("etl", NOW - timedelta(hours=42)),
    ]
    result = compute_momentum(runs, "etl", window_hours=24.0, now=NOW)
    assert result.trend == "stable"
    assert result.delta == 0


def test_compute_momentum_insufficient_data():
    runs = [make_run("etl", NOW - timedelta(hours=1))]
    result = compute_momentum(runs, "etl", window_hours=24.0, min_runs=2, now=NOW)
    assert result.trend == "insufficient_data"


def test_compute_momentum_no_runs():
    result = compute_momentum([], "etl", window_hours=24.0, now=NOW)
    assert result.trend == "insufficient_data"
    assert result.recent_run_count == 0
    assert result.prior_run_count == 0


def test_compute_momentum_filters_by_pipeline():
    runs = [
        make_run("etl", NOW - timedelta(hours=2)),
        make_run("other", NOW - timedelta(hours=3)),
        make_run("etl", NOW - timedelta(hours=30)),
        make_run("other", NOW - timedelta(hours=31)),
    ]
    result = compute_momentum(runs, "etl", window_hours=24.0, now=NOW)
    assert result.pipeline == "etl"
    assert result.recent_run_count == 1
    assert result.prior_run_count == 1


def test_str_representation():
    runs = [
        make_run("pipe", NOW - timedelta(hours=1)),
        make_run("pipe", NOW - timedelta(hours=2)),
        make_run("pipe", NOW - timedelta(hours=25)),
    ]
    result = compute_momentum(runs, "pipe", window_hours=24.0, now=NOW)
    text = str(result)
    assert "pipe" in text
    assert "accelerating" in text


def test_compute_all_momentums_returns_one_per_pipeline():
    runs = [
        make_run("a", NOW - timedelta(hours=1)),
        make_run("a", NOW - timedelta(hours=25)),
        make_run("b", NOW - timedelta(hours=2)),
        make_run("b", NOW - timedelta(hours=26)),
    ]
    results = compute_all_momentums(runs, window_hours=24.0, now=NOW)
    pipelines = [r.pipeline for r in results]
    assert sorted(pipelines) == ["a", "b"]


def test_compute_all_momentums_empty():
    results = compute_all_momentums([], now=NOW)
    assert results == []
