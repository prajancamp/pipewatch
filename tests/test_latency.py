"""Tests for pipewatch.latency."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from pipewatch.latency import (
    LatencyResult,
    _percentile,
    compute_latency,
    compute_all_latencies,
)
from pipewatch.models import PipelineRun, PipelineStatus


def make_run(
    pipeline: str = "pipe",
    status: PipelineStatus = PipelineStatus.SUCCESS,
    duration: float | None = 10.0,
) -> PipelineRun:
    now = datetime.now(timezone.utc).isoformat()
    return PipelineRun(
        run_id=str(uuid.uuid4()),
        pipeline=pipeline,
        status=status,
        started_at=now,
        ended_at=now,
        duration=duration,
    )


# --- _percentile ---

def test_percentile_basic():
    data = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert _percentile(data, 0) == 1.0
    assert _percentile(data, 100) == 5.0
    assert _percentile(data, 50) == 3.0


def test_percentile_single_element():
    assert _percentile([7.0], 50) == 7.0
    assert _percentile([7.0], 99) == 7.0


def test_percentile_empty():
    assert _percentile([], 50) == 0.0


# --- compute_latency ---

def test_compute_latency_no_runs():
    result = compute_latency([], "pipe")
    assert result.pipeline == "pipe"
    assert result.sample_size == 0
    assert result.p50 is None
    assert result.p95 is None
    assert result.p99 is None


def test_compute_latency_no_duration():
    runs = [make_run(duration=None) for _ in range(5)]
    result = compute_latency(runs, "pipe")
    assert result.sample_size == 0
    assert result.p50 is None


def test_compute_latency_single_run():
    runs = [make_run(duration=42.0)]
    result = compute_latency(runs, "pipe")
    assert result.sample_size == 1
    assert result.p50 == pytest.approx(42.0)
    assert result.p95 == pytest.approx(42.0)
    assert result.p99 == pytest.approx(42.0)
    assert result.min_duration == pytest.approx(42.0)
    assert result.max_duration == pytest.approx(42.0)


def test_compute_latency_percentile_ordering():
    durations = [float(i) for i in range(1, 101)]  # 1..100
    runs = [make_run(duration=d) for d in durations]
    result = compute_latency(runs, "pipe")
    assert result.sample_size == 100
    assert result.p50 == pytest.approx(50.5)
    assert result.p95 == pytest.approx(95.05)
    assert result.min_duration == pytest.approx(1.0)
    assert result.max_duration == pytest.approx(100.0)


def test_compute_latency_filters_by_pipeline():
    runs = [
        make_run(pipeline="a", duration=10.0),
        make_run(pipeline="b", duration=999.0),
    ]
    result = compute_latency(runs, "a")
    assert result.sample_size == 1
    assert result.p50 == pytest.approx(10.0)


# --- compute_all_latencies ---

def test_compute_all_latencies_groups_pipelines():
    runs = [
        make_run(pipeline="alpha", duration=5.0),
        make_run(pipeline="alpha", duration=15.0),
        make_run(pipeline="beta", duration=30.0),
    ]
    results = compute_all_latencies(runs)
    assert set(results.keys()) == {"alpha", "beta"}
    assert results["alpha"].sample_size == 2
    assert results["beta"].sample_size == 1


def test_compute_all_latencies_filter_pipeline():
    runs = [
        make_run(pipeline="alpha", duration=5.0),
        make_run(pipeline="beta", duration=30.0),
    ]
    results = compute_all_latencies(runs, pipeline="alpha")
    assert list(results.keys()) == ["alpha"]


def test_latency_result_str_with_data():
    r = LatencyResult(
        pipeline="my_pipe", sample_size=10,
        p50=1.0, p95=2.0, p99=3.0,
        min_duration=0.5, max_duration=4.0,
    )
    s = str(r)
    assert "my_pipe" in s
    assert "p50" in s
    assert "p95" in s


def test_latency_result_str_no_data():
    r = LatencyResult(
        pipeline="empty", sample_size=0,
        p50=None, p95=None, p99=None,
        min_duration=None, max_duration=None,
    )
    assert "no duration data" in str(r)
