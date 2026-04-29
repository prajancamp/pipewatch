"""Tests for pipewatch.saturation."""
from __future__ import annotations

import time
from typing import List

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.saturation import check_saturation, SaturationResult


NOW = 1_700_000_000.0


def make_run(
    pipeline: str = "pipe",
    status: PipelineStatus = PipelineStatus.SUCCESS,
    started_at: float = NOW - 60,
    duration: float = 10.0,
) -> PipelineRun:
    return PipelineRun(
        run_id=f"r-{started_at}",
        pipeline=pipeline,
        status=status,
        started_at=started_at,
        ended_at=started_at + duration,
    )


def test_check_saturation_empty_returns_empty():
    results = check_saturation([], max_runs=5, window_hours=1, _now=NOW)
    assert results == []


def test_check_saturation_below_max_not_saturated():
    runs = [make_run(started_at=NOW - 100) for _ in range(3)]
    results = check_saturation(runs, max_runs=10, window_hours=1, _now=NOW)
    assert len(results) == 1
    r = results[0]
    assert r.run_count == 3
    assert r.max_runs == 10
    assert not r.is_saturated
    assert pytest.approx(r.utilization) == 0.3


def test_check_saturation_at_max_is_saturated():
    runs = [make_run(started_at=NOW - 100) for _ in range(5)]
    results = check_saturation(runs, max_runs=5, window_hours=1, _now=NOW)
    assert results[0].is_saturated
    assert pytest.approx(results[0].utilization) == 1.0


def test_check_saturation_over_max_is_saturated():
    runs = [make_run(started_at=NOW - 100) for _ in range(8)]
    results = check_saturation(runs, max_runs=5, window_hours=1, _now=NOW)
    r = results[0]
    assert r.is_saturated
    assert r.utilization > 1.0


def test_check_saturation_excludes_old_runs():
    old = make_run(started_at=NOW - 7200)  # 2 h ago, outside 1-h window
    recent = make_run(started_at=NOW - 100)
    results = check_saturation([old, recent], max_runs=5, window_hours=1, _now=NOW)
    assert results[0].run_count == 1


def test_check_saturation_avg_duration():
    runs = [
        make_run(started_at=NOW - 100, duration=20.0),
        make_run(started_at=NOW - 200, duration=40.0),
    ]
    results = check_saturation(runs, max_runs=10, window_hours=1, _now=NOW)
    assert pytest.approx(results[0].avg_duration_s) == 30.0


def test_check_saturation_no_duration():
    run = PipelineRun(
        run_id="r1",
        pipeline="pipe",
        status=PipelineStatus.SUCCESS,
        started_at=NOW - 100,
        ended_at=None,
    )
    results = check_saturation([run], max_runs=5, window_hours=1, _now=NOW)
    assert results[0].avg_duration_s is None


def test_check_saturation_filter_pipeline():
    runs = [
        make_run(pipeline="a", started_at=NOW - 100),
        make_run(pipeline="b", started_at=NOW - 100),
    ]
    results = check_saturation(runs, max_runs=5, window_hours=1, pipeline="a", _now=NOW)
    assert len(results) == 1
    assert results[0].pipeline == "a"


def test_saturation_str_contains_pipeline():
    r = SaturationResult(
        pipeline="etl",
        window_hours=1,
        run_count=7,
        max_runs=10,
        utilization=0.7,
        avg_duration_s=15.5,
        is_saturated=False,
    )
    s = str(r)
    assert "etl" in s
    assert "7/10" in s
    assert "70.0%" in s
