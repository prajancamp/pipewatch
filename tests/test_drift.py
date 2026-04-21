"""Tests for pipewatch.drift."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import List

import pytest

from pipewatch.drift import detect_drift, detect_all_drift, DriftResult
from pipewatch.models import PipelineRun, PipelineStatus


def make_run(
    pipeline: str = "etl",
    status: PipelineStatus = PipelineStatus.SUCCESS,
    duration: float = 30.0,
    offset_hours: int = 0,
) -> PipelineRun:
    start = datetime(2024, 1, 1, 0, 0, 0) + timedelta(hours=offset_hours)
    end = start + timedelta(seconds=duration)
    return PipelineRun(
        pipeline=pipeline,
        status=status,
        started_at=start,
        finished_at=end,
    )


def _make_window(pipeline: str, status: PipelineStatus, duration: float, count: int, start_offset: int = 0) -> List[PipelineRun]:
    return [
        make_run(pipeline, status, duration, offset_hours=start_offset + i)
        for i in range(count)
    ]


def test_insufficient_data_returns_empty():
    runs = _make_window("etl", PipelineStatus.SUCCESS, 30.0, 15)
    # Need at least window_size * 2 = 20 runs
    result = detect_drift(runs, "etl", window_size=10)
    assert result == []


def test_no_drift_stable_pipeline():
    runs = (
        _make_window("etl", PipelineStatus.SUCCESS, 30.0, 10, start_offset=0)
        + _make_window("etl", PipelineStatus.SUCCESS, 31.0, 10, start_offset=10)
    )
    results = detect_drift(runs, "etl", window_size=10, threshold_pct=20.0)
    assert all(not r.flagged for r in results)


def test_success_rate_drift_flagged():
    # Baseline: all success; Current: all failure → 100% drop
    baseline = _make_window("etl", PipelineStatus.SUCCESS, 30.0, 10, start_offset=0)
    current = _make_window("etl", PipelineStatus.FAILED, 30.0, 10, start_offset=10)
    runs = baseline + current
    results = detect_drift(runs, "etl", window_size=10, threshold_pct=20.0)
    sr_results = [r for r in results if r.metric == "success_rate"]
    assert len(sr_results) == 1
    assert sr_results[0].flagged
    assert sr_results[0].delta < 0


def test_duration_drift_flagged():
    # Baseline: 10s; Current: 100s → 900% increase
    baseline = _make_window("etl", PipelineStatus.SUCCESS, 10.0, 10, start_offset=0)
    current = _make_window("etl", PipelineStatus.SUCCESS, 100.0, 10, start_offset=10)
    runs = baseline + current
    results = detect_drift(runs, "etl", window_size=10, threshold_pct=20.0)
    dur_results = [r for r in results if r.metric == "avg_duration"]
    assert len(dur_results) == 1
    assert dur_results[0].flagged
    assert dur_results[0].delta > 0


def test_detect_all_drift_multiple_pipelines():
    runs_a = (
        _make_window("alpha", PipelineStatus.SUCCESS, 10.0, 10, start_offset=0)
        + _make_window("alpha", PipelineStatus.SUCCESS, 90.0, 10, start_offset=10)
    )
    runs_b = (
        _make_window("beta", PipelineStatus.SUCCESS, 20.0, 10, start_offset=0)
        + _make_window("beta", PipelineStatus.SUCCESS, 21.0, 10, start_offset=10)
    )
    results = detect_all_drift(runs_a + runs_b, window_size=10, threshold_pct=20.0)
    pipelines_flagged = {r.pipeline for r in results if r.flagged}
    assert "alpha" in pipelines_flagged
    assert "beta" not in pipelines_flagged


def test_drift_result_str_contains_pipeline():
    r = DriftResult(
        pipeline="my_pipe",
        metric="success_rate",
        baseline_value=1.0,
        current_value=0.5,
        delta=-0.5,
        pct_change=-50.0,
        flagged=True,
    )
    s = str(r)
    assert "my_pipe" in s
    assert "DRIFT" in s
    assert "success_rate" in s
