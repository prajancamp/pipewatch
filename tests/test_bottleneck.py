"""Tests for pipewatch.bottleneck."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest

from pipewatch.bottleneck import BottleneckResult, _percentile, detect_bottlenecks
from pipewatch.models import PipelineRun, PipelineStatus


def make_run(
    pipeline: str = "etl",
    duration: Optional[float] = 60.0,
    status: PipelineStatus = PipelineStatus.SUCCESS,
) -> PipelineRun:
    now = datetime.now(timezone.utc).isoformat()
    return PipelineRun(
        run_id="r1",
        pipeline=pipeline,
        status=status,
        started_at=now,
        ended_at=now,
        duration=duration,
    )


def test_percentile_basic():
    assert _percentile([10, 20, 30, 40, 50, 60, 70, 80, 90, 100], 90) == 90


def test_percentile_empty():
    assert _percentile([], 90) == 0.0


def test_detect_bottlenecks_empty():
    assert detect_bottlenecks([]) == []


def test_detect_bottlenecks_insufficient_runs():
    runs = [make_run(duration=500.0) for _ in range(2)]
    results = detect_bottlenecks(runs, threshold=100.0, min_runs=3)
    assert results == []


def test_detect_bottlenecks_flags_slow_pipeline():
    runs = [make_run(pipeline="slow", duration=float(d)) for d in [400, 420, 410, 430, 440]]
    results = detect_bottlenecks(runs, threshold=300.0, min_runs=3)
    assert len(results) == 1
    r = results[0]
    assert r.pipeline == "slow"
    assert r.is_bottleneck is True
    assert r.p90_duration > 300.0


def test_detect_bottlenecks_ok_pipeline():
    runs = [make_run(pipeline="fast", duration=float(d)) for d in [10, 15, 12, 11, 13]]
    results = detect_bottlenecks(runs, threshold=300.0, min_runs=3)
    assert len(results) == 1
    assert results[0].is_bottleneck is False


def test_detect_bottlenecks_skips_none_duration():
    runs = [make_run(duration=None) for _ in range(5)]
    results = detect_bottlenecks(runs, min_runs=3)
    assert results == []


def test_detect_bottlenecks_filter_pipeline():
    runs = [
        make_run(pipeline="a", duration=500.0),
        make_run(pipeline="a", duration=500.0),
        make_run(pipeline="a", duration=500.0),
        make_run(pipeline="b", duration=500.0),
        make_run(pipeline="b", duration=500.0),
        make_run(pipeline="b", duration=500.0),
    ]
    results = detect_bottlenecks(runs, threshold=100.0, min_runs=3, pipeline="a")
    assert all(r.pipeline == "a" for r in results)
    assert len(results) == 1


def test_bottleneck_result_str_flagged():
    r = BottleneckResult(
        pipeline="slow_pipe",
        avg_duration=400.0,
        max_duration=500.0,
        p90_duration=480.0,
        run_count=10,
        threshold=300.0,
        is_bottleneck=True,
    )
    assert "[BOTTLENECK]" in str(r)
    assert "slow_pipe" in str(r)


def test_bottleneck_result_str_ok():
    r = BottleneckResult(
        pipeline="fast_pipe",
        avg_duration=20.0,
        max_duration=30.0,
        p90_duration=25.0,
        run_count=5,
        threshold=300.0,
        is_bottleneck=False,
    )
    assert "[ok]" in str(r)
