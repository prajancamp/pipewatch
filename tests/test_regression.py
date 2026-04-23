"""Tests for pipewatch.regression."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.regression import detect_regression, RegressionResult


def make_run(
    pipeline: str,
    status: PipelineStatus = PipelineStatus.SUCCESS,
    started_at: str = "2024-01-01T00:00:00",
    duration: float = 10.0,
) -> PipelineRun:
    return PipelineRun(
        run_id=str(uuid.uuid4()),
        pipeline=pipeline,
        status=status,
        started_at=started_at,
        finished_at=started_at,
        duration=duration,
        error=None,
        meta={},
        tags=[],
    )


def _ts(hour: int) -> str:
    return f"2024-01-01T{hour:02d}:00:00"


def _make_runs(
    pipeline: str,
    n_baseline: int,
    n_recent: int,
    baseline_status: PipelineStatus = PipelineStatus.SUCCESS,
    recent_status: PipelineStatus = PipelineStatus.FAILED,
    baseline_duration: float = 10.0,
    recent_duration: float = 10.0,
) -> List[PipelineRun]:
    runs = []
    for i in range(n_baseline):
        runs.append(make_run(pipeline, baseline_status, _ts(i), baseline_duration))
    for i in range(n_baseline, n_baseline + n_recent):
        runs.append(make_run(pipeline, recent_status, _ts(i), recent_duration))
    return runs


def test_detect_regression_success_rate_drop():
    runs = _make_runs(
        "etl",
        n_baseline=20,
        n_recent=10,
        baseline_status=PipelineStatus.SUCCESS,
        recent_status=PipelineStatus.FAILED,
    )
    results = detect_regression(runs, baseline_window=20, recent_window=10)
    assert len(results) == 1
    r = results[0]
    assert r.regressed is True
    assert "success rate" in r.reason
    assert r.baseline_success_rate == 1.0
    assert r.recent_success_rate == 0.0


def test_no_regression_when_stable():
    runs = _make_runs(
        "etl",
        n_baseline=20,
        n_recent=10,
        baseline_status=PipelineStatus.SUCCESS,
        recent_status=PipelineStatus.SUCCESS,
    )
    results = detect_regression(runs, baseline_window=20, recent_window=10)
    assert len(results) == 1
    assert results[0].regressed is False
    assert results[0].reason == "within normal range"


def test_regression_on_duration_increase():
    runs = _make_runs(
        "etl",
        n_baseline=20,
        n_recent=10,
        baseline_status=PipelineStatus.SUCCESS,
        recent_status=PipelineStatus.SUCCESS,
        baseline_duration=10.0,
        recent_duration=15.0,  # 50% increase, threshold is 25%
    )
    results = detect_regression(runs, baseline_window=20, recent_window=10)
    assert len(results) == 1
    assert results[0].regressed is True
    assert "duration" in results[0].reason


def test_insufficient_data_skipped():
    runs = _make_runs("etl", n_baseline=5, n_recent=5)
    results = detect_regression(runs, baseline_window=20, recent_window=10)
    assert results == []


def test_filter_by_pipeline():
    runs = _make_runs("etl", 20, 10, recent_status=PipelineStatus.FAILED)
    runs += _make_runs("other", 20, 10, recent_status=PipelineStatus.FAILED)
    results = detect_regression(runs, pipeline="etl")
    assert all(r.pipeline == "etl" for r in results)


def test_str_output_contains_pipeline_name():
    runs = _make_runs("my_pipe", 20, 10, recent_status=PipelineStatus.FAILED)
    results = detect_regression(runs)
    assert len(results) == 1
    assert "my_pipe" in str(results[0])
    assert "REGRESSED" in str(results[0])
