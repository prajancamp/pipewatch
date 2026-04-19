"""Tests for pipewatch.forecast"""

from __future__ import annotations
import pytest
from datetime import datetime, timezone
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.forecast import forecast_pipeline, forecast_all, _failure_rate


def make_run(pipeline: str, status: PipelineStatus, ts: float = 0.0) -> PipelineRun:
    return PipelineRun(
        run_id=f"{pipeline}-{ts}",
        pipeline=pipeline,
        status=status,
        started_at=datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        ended_at=datetime.fromtimestamp(ts + 1, tz=timezone.utc).isoformat(),
    )


def test_failure_rate_all_success():
    runs = [make_run("p", PipelineStatus.SUCCESS, i) for i in range(5)]
    assert _failure_rate(runs) == 0.0


def test_failure_rate_all_failed():
    runs = [make_run("p", PipelineStatus.FAILED, i) for i in range(5)]
    assert _failure_rate(runs) == 1.0


def test_failure_rate_empty():
    assert _failure_rate([]) == 0.0


def test_forecast_insufficient_data():
    runs = [make_run("pipe", PipelineStatus.SUCCESS, i) for i in range(3)]
    result = forecast_pipeline(runs, "pipe")
    assert result is None


def test_forecast_stable_pipeline():
    runs = [make_run("pipe", PipelineStatus.SUCCESS, i) for i in range(20)]
    result = forecast_pipeline(runs, "pipe")
    assert result is not None
    assert result.predicted_failure_rate == pytest.approx(0.0)
    assert result.recent_failure_rate == 0.0
    assert result.older_failure_rate == 0.0


def test_forecast_degrading_pipeline():
    older = [make_run("pipe", PipelineStatus.SUCCESS, i) for i in range(10)]
    recent = [make_run("pipe", PipelineStatus.FAILED, i + 10) for i in range(10)]
    result = forecast_pipeline(older + recent, "pipe")
    assert result is not None
    assert result.predicted_failure_rate > result.recent_failure_rate


def test_forecast_improving_pipeline():
    older = [make_run("pipe", PipelineStatus.FAILED, i) for i in range(10)]
    recent = [make_run("pipe", PipelineStatus.SUCCESS, i + 10) for i in range(10)]
    result = forecast_pipeline(older + recent, "pipe")
    assert result is not None
    assert result.predicted_failure_rate < result.recent_failure_rate


def test_forecast_all_returns_one_per_pipeline():
    runs = (
        [make_run("a", PipelineStatus.SUCCESS, i) for i in range(10)]
        + [make_run("b", PipelineStatus.FAILED, i) for i in range(10)]
    )
    results = forecast_all(runs)
    names = [r.pipeline for r in results]
    assert "a" in names
    assert "b" in names


def test_forecast_str_contains_pipeline():
    runs = [make_run("mypipe", PipelineStatus.SUCCESS, i) for i in range(20)]
    result = forecast_pipeline(runs, "mypipe")
    assert result is not None
    assert "mypipe" in str(result)
