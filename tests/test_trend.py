"""Tests for pipewatch.trend module."""
import pytest
from datetime import datetime, timedelta
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.trend import compute_trend, compute_all_trends, TrendResult


def make_run(pipeline: str, success: bool, offset_minutes: int = 0) -> PipelineRun:
    start = datetime(2024, 1, 1, 0, 0, 0) + timedelta(minutes=offset_minutes)
    end = start + timedelta(minutes=1)
    return PipelineRun(
        pipeline=pipeline,
        status=PipelineStatus.SUCCESS if success else PipelineStatus.FAILED,
        started_at=start.isoformat(),
        ended_at=end.isoformat(),
    )


def test_compute_trend_improving():
    runs = (
        [make_run("etl", False, i) for i in range(5)] +
        [make_run("etl", True, i + 5) for i in range(5)]
    )
    result = compute_trend(runs, "etl", window=5)
    assert result is not None
    assert result.verdict == "improving"
    assert result.delta > 0


def test_compute_trend_degrading():
    runs = (
        [make_run("etl", True, i) for i in range(5)] +
        [make_run("etl", False, i + 5) for i in range(5)]
    )
    result = compute_trend(runs, "etl", window=5)
    assert result is not None
    assert result.verdict == "degrading"
    assert result.delta < 0


def test_compute_trend_stable():
    runs = [make_run("etl", i % 2 == 0, i) for i in range(10)]
    result = compute_trend(runs, "etl", window=5, threshold=0.5)
    assert result is not None
    assert result.verdict == "stable"


def test_compute_trend_insufficient_data():
    runs = [make_run("etl", True, i) for i in range(7)]
    result = compute_trend(runs, "etl", window=5)
    assert result is None


def test_compute_all_trends_multiple_pipelines():
    runs = (
        [make_run("a", False, i) for i in range(5)] +
        [make_run("a", True, i + 5) for i in range(5)] +
        [make_run("b", True, i) for i in range(5)] +
        [make_run("b", False, i + 5) for i in range(5)]
    )
    results = compute_all_trends(runs, window=5)
    assert len(results) == 2
    verdicts = {r.pipeline: r.verdict for r in results}
    assert verdicts["a"] == "improving"
    assert verdicts["b"] == "degrading"


def test_trend_str():
    t = TrendResult("etl", 0.4, 0.9, 0.5, "improving")
    assert "↑" in str(t)
    assert "etl" in str(t)


def test_compute_all_trends_empty():
    assert compute_all_trends([]) == []
