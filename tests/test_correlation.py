"""Tests for pipewatch.correlation"""
import pytest
from datetime import datetime, timezone
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.correlation import compute_correlations, CorrelationResult


def make_run(pipeline, status, started_at, run_id=None):
    import uuid
    return PipelineRun(
        run_id=run_id or str(uuid.uuid4()),
        pipeline=pipeline,
        status=status,
        started_at=started_at,
        ended_at=started_at,
    )


T0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def ts(minutes_offset):
    from datetime import timedelta
    return T0.replace(tzinfo=None) + __import__("datetime").timedelta(minutes=minutes_offset)


def test_no_correlations_when_no_failures():
    runs = [
        make_run("a", PipelineStatus.SUCCESS, T0),
        make_run("b", PipelineStatus.SUCCESS, T0),
    ]
    assert compute_correlations(runs) == []


def test_correlated_failures_within_window():
    from datetime import timedelta
    t1 = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    t2 = t1 + timedelta(minutes=2)
    runs = [
        make_run("alpha", PipelineStatus.FAILED, t1),
        make_run("beta", PipelineStatus.FAILED, t2),
    ]
    results = compute_correlations(runs, window_minutes=5)
    assert len(results) == 1
    r = results[0]
    assert r.pipeline_a == "alpha"
    assert r.pipeline_b == "beta"
    assert r.co_failures == 1


def test_no_correlation_outside_window():
    from datetime import timedelta
    t1 = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    t2 = t1 + timedelta(minutes=10)
    runs = [
        make_run("alpha", PipelineStatus.FAILED, t1),
        make_run("beta", PipelineStatus.FAILED, t2),
    ]
    results = compute_correlations(runs, window_minutes=5)
    assert results == []


def test_multiple_co_failures_counted():
    from datetime import timedelta
    results_list = []
    runs = []
    base = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    for i in range(3):
        runs.append(make_run("pipe_x", PipelineStatus.FAILED, base + timedelta(hours=i)))
        runs.append(make_run("pipe_y", PipelineStatus.FAILED, base + timedelta(hours=i, minutes=1)))
    results = compute_correlations(runs, window_minutes=5)
    assert len(results) == 1
    assert results[0].co_failures == 3


def test_str_representation():
    r = CorrelationResult("a", "b", 4, 5)
    assert "a" in str(r)
    assert "b" in str(r)
    assert "4" in str(r)
    assert "5m" in str(r)
