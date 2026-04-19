"""Tests for pipewatch.analyzer module."""
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.analyzer import compute_stats, find_consecutive_failures


def make_run(name: str, status: PipelineStatus, started_at: datetime = None) -> PipelineRun:
    now = started_at or datetime.now(timezone.utc)
    return PipelineRun(
        run_id=str(uuid4()),
        pipeline_name=name,
        status=status,
        started_at=now,
        finished_at=now,
    )


def test_compute_stats_basic():
    runs = [
        make_run("etl_daily", PipelineStatus.SUCCESS),
        make_run("etl_daily", PipelineStatus.FAILED),
        make_run("etl_daily", PipelineStatus.SUCCESS),
    ]
    stats = compute_stats(runs)
    assert "etl_daily" in stats
    s = stats["etl_daily"]
    assert s.total_runs == 3
    assert s.failed_runs == 1
    assert s.success_runs == 2
    assert pytest.approx(s.failure_rate, 0.01) == 1 / 3


def test_compute_stats_multiple_pipelines():
    runs = [
        make_run("pipe_a", PipelineStatus.SUCCESS),
        make_run("pipe_b", PipelineStatus.FAILED),
        make_run("pipe_b", PipelineStatus.FAILED),
    ]
    stats = compute_stats(runs)
    assert stats["pipe_a"].total_runs == 1
    assert stats["pipe_b"].failure_rate == 1.0


def test_compute_stats_empty():
    assert compute_stats([]) == {}


def test_compute_stats_all_failures():
    runs = [
        make_run("batch", PipelineStatus.FAILED),
        make_run("batch", PipelineStatus.FAILED),
    ]
    stats = compute_stats(runs)
    assert stats["batch"].failure_rate == 1.0
    assert stats["batch"].success_runs == 0


def test_find_consecutive_failures_detected():
    runs = [
        make_run("nightly", PipelineStatus.SUCCESS),
        make_run("nightly", PipelineStatus.FAILED),
        make_run("nightly", PipelineStatus.FAILED),
        make_run("nightly", PipelineStatus.FAILED),
    ]
    flagged = find_consecutive_failures(runs, threshold=3)
    assert "nightly" in flagged


def test_find_consecutive_failures_not_triggered():
    runs = [
        make_run("nightly", PipelineStatus.FAILED),
        make_run("nightly", PipelineStatus.FAILED),
        make_run("nightly", PipelineStatus.SUCCESS),
    ]
    flagged = find_consecutive_failures(runs, threshold=3)
    assert "nightly" not in flagged


def test_find_consecutive_failures_empty():
    assert find_consecutive_failures([]) == []
