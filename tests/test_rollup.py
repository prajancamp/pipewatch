"""Tests for pipewatch.rollup."""
from __future__ import annotations
import time
from datetime import datetime, timezone
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.rollup import compute_rollup, RollupBucket


def make_run(
    pipeline: str = "etl",
    status: PipelineStatus = PipelineStatus.SUCCESS,
    started_at: float | None = None,
    duration: float | None = 10.0,
) -> PipelineRun:
    ts = started_at or time.time()
    return PipelineRun(
        run_id=f"r-{ts}-{pipeline}",
        pipeline=pipeline,
        status=status,
        started_at=ts,
        duration=duration,
    )


DAY1 = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc).timestamp()
DAY2 = datetime(2024, 1, 16, 9, 0, 0, tzinfo=timezone.utc).timestamp()
DAY1_HR2 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp()


def test_compute_rollup_daily_groups():
    runs = [
        make_run(started_at=DAY1),
        make_run(started_at=DAY1, status=PipelineStatus.FAILED),
        make_run(started_at=DAY2),
    ]
    result = compute_rollup(runs, "daily")
    assert "2024-01-15" in result
    assert "2024-01-16" in result
    assert result["2024-01-15"]["etl"].total == 2
    assert result["2024-01-16"]["etl"].total == 1


def test_compute_rollup_hourly_groups():
    runs = [
        make_run(started_at=DAY1),
        make_run(started_at=DAY1_HR2),
    ]
    result = compute_rollup(runs, "hourly")
    assert "2024-01-15T10" in result
    assert "2024-01-15T12" in result


def test_compute_rollup_counts_failures():
    runs = [
        make_run(started_at=DAY1, status=PipelineStatus.SUCCESS),
        make_run(started_at=DAY1, status=PipelineStatus.FAILED),
        make_run(started_at=DAY1, status=PipelineStatus.FAILED),
    ]
    result = compute_rollup(runs)
    b = result["2024-01-15"]["etl"]
    assert b.failures == 2
    assert b.successes == 1
    assert abs(b.success_rate - 1 / 3) < 0.01


def test_compute_rollup_avg_duration():
    runs = [
        make_run(started_at=DAY1, duration=10.0),
        make_run(started_at=DAY1, duration=20.0),
    ]
    result = compute_rollup(runs)
    b = result["2024-01-15"]["etl"]
    assert b.avg_duration == 15.0


def test_compute_rollup_no_duration():
    runs = [make_run(started_at=DAY1, duration=None)]
    result = compute_rollup(runs)
    b = result["2024-01-15"]["etl"]
    assert b.avg_duration is None


def test_compute_rollup_empty():
    assert compute_rollup([]) == {}


def test_compute_rollup_invalid_granularity():
    import pytest
    with pytest.raises(ValueError):
        compute_rollup([], granularity="weekly")


def test_rollup_bucket_str():
    b = RollupBucket("2024-01-15", "etl", 3, 1, 2, 12.5)
    s = str(b)
    assert "2024-01-15" in s
    assert "etl" in s
    assert "12.5s" in s
