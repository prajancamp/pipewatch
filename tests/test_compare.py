"""Tests for pipewatch.compare."""
from datetime import datetime, timezone
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.compare import compare_windows, WindowComparison


def make_run(pipeline, status, started_at, duration=10.0, run_id=None):
    import uuid
    return PipelineRun(
        run_id=run_id or str(uuid.uuid4()),
        pipeline=pipeline,
        status=status,
        started_at=started_at,
        duration=duration,
    )


T = lambda d, h=0: datetime(2024, 1, d, h, tzinfo=timezone.utc)


def test_compare_basic_improvement():
    runs = [
        make_run("etl", PipelineStatus.FAILED, T(1)),
        make_run("etl", PipelineStatus.FAILED, T(2)),
        make_run("etl", PipelineStatus.SUCCESS, T(5)),
        make_run("etl", PipelineStatus.SUCCESS, T(6)),
        make_run("etl", PipelineStatus.SUCCESS, T(7)),
        make_run("etl", PipelineStatus.SUCCESS, T(8)),
    ]
    results = compare_windows(runs, T(1), T(3), T(5), T(9))
    assert len(results) == 1
    c = results[0]
    assert c.pipeline == "etl"
    assert c.before_success_rate == 0.0
    assert c.after_success_rate == 100.0
    assert c.success_rate_delta == 100.0


def test_compare_missing_before():
    runs = [
        make_run("new_pipe", PipelineStatus.SUCCESS, T(5)),
        make_run("new_pipe", PipelineStatus.SUCCESS, T(6)),
    ]
    results = compare_windows(runs, T(1), T(3), T(5), T(9))
    assert len(results) == 1
    c = results[0]
    assert c.before_success_rate is None
    assert c.after_success_rate == 100.0
    assert c.success_rate_delta is None
    assert c.before_total == 0
    assert c.after_total == 2


def test_compare_duration_delta():
    runs = [
        make_run("pipe", PipelineStatus.SUCCESS, T(1), duration=20.0),
        make_run("pipe", PipelineStatus.SUCCESS, T(5), duration=10.0),
    ]
    results = compare_windows(runs, T(1), T(3), T(5), T(9))
    c = results[0]
    assert c.duration_delta == -10.0


def test_compare_multiple_pipelines():
    runs = [
        make_run("a", PipelineStatus.SUCCESS, T(1)),
        make_run("b", PipelineStatus.FAILED, T(1)),
        make_run("a", PipelineStatus.SUCCESS, T(5)),
        make_run("b", PipelineStatus.SUCCESS, T(5)),
    ]
    results = compare_windows(runs, T(1), T(3), T(5), T(9))
    names = [r.pipeline for r in results]
    assert names == ["a", "b"]


def test_str_representation():
    c = WindowComparison(
        pipeline="etl",
        before_success_rate=50.0,
        after_success_rate=75.0,
        before_avg_duration=30.0,
        after_avg_duration=20.0,
        before_total=4,
        after_total=8,
    )
    s = str(c)
    assert "etl" in s
    assert "+25.0%" in s
    assert "-10.0s" in s
