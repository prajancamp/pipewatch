"""Tests for pipewatch.mttr MTTR analysis."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.mttr import MTTRResult, compute_mttr, compute_all_mttr


def make_run(
    pipeline: str,
    status: PipelineStatus,
    offset_minutes: int,
    duration: Optional[float] = 1.0,
    error: Optional[str] = None,
) -> PipelineRun:
    base = datetime(2024, 1, 1, 0, 0, 0)
    started_at = base + timedelta(minutes=offset_minutes)
    finished_at = started_at + timedelta(minutes=duration) if duration else None
    return PipelineRun(
        run_id=f"{pipeline}-{offset_minutes}",
        pipeline=pipeline,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        error=error,
    )


def test_compute_mttr_no_runs():
    result = compute_mttr([], "pipe-a")
    assert result is None


def test_compute_mttr_no_failures():
    runs = [make_run("pipe-a", PipelineStatus.SUCCESS, i * 10) for i in range(5)]
    result = compute_mttr(runs, "pipe-a")
    assert result is not None
    assert result.total_incidents == 0
    assert result.recovered_incidents == 0
    assert result.mean_recovery_minutes is None


def test_compute_mttr_single_recovery():
    runs = [
        make_run("pipe-a", PipelineStatus.SUCCESS, 0),
        make_run("pipe-a", PipelineStatus.FAILED, 10, error="boom"),
        make_run("pipe-a", PipelineStatus.SUCCESS, 30),
    ]
    result = compute_mttr(runs, "pipe-a")
    assert result is not None
    assert result.total_incidents == 1
    assert result.recovered_incidents == 1
    # recovery = 30 - 10 = 20 minutes
    assert result.mean_recovery_minutes == pytest.approx(20.0)
    assert result.longest_recovery_minutes == pytest.approx(20.0)
    assert result.shortest_recovery_minutes == pytest.approx(20.0)


def test_compute_mttr_consecutive_failures_one_incident():
    runs = [
        make_run("pipe-a", PipelineStatus.FAILED, 0, error="e1"),
        make_run("pipe-a", PipelineStatus.FAILED, 10, error="e2"),
        make_run("pipe-a", PipelineStatus.SUCCESS, 40),
    ]
    result = compute_mttr(runs, "pipe-a")
    assert result is not None
    # First failure at 0, recovery at 40 => 40 minutes
    assert result.total_incidents == 1
    assert result.recovered_incidents == 1
    assert result.mean_recovery_minutes == pytest.approx(40.0)


def test_compute_mttr_unrecovered_incident():
    runs = [
        make_run("pipe-a", PipelineStatus.SUCCESS, 0),
        make_run("pipe-a", PipelineStatus.FAILED, 10, error="boom"),
    ]
    result = compute_mttr(runs, "pipe-a")
    assert result is not None
    assert result.total_incidents == 1
    assert result.recovered_incidents == 0
    assert result.mean_recovery_minutes is None


def test_compute_mttr_multiple_recoveries():
    runs = [
        make_run("pipe-a", PipelineStatus.FAILED, 0, error="e"),
        make_run("pipe-a", PipelineStatus.SUCCESS, 20),
        make_run("pipe-a", PipelineStatus.FAILED, 30, error="e"),
        make_run("pipe-a", PipelineStatus.SUCCESS, 50),
    ]
    result = compute_mttr(runs, "pipe-a")
    assert result is not None
    assert result.total_incidents == 2
    assert result.recovered_incidents == 2
    assert result.mean_recovery_minutes == pytest.approx(20.0)
    assert result.longest_recovery_minutes == pytest.approx(20.0)
    assert result.shortest_recovery_minutes == pytest.approx(20.0)


def test_compute_all_mttr_multiple_pipelines():
    runs = [
        make_run("pipe-a", PipelineStatus.FAILED, 0, error="e"),
        make_run("pipe-a", PipelineStatus.SUCCESS, 15),
        make_run("pipe-b", PipelineStatus.FAILED, 0, error="e"),
        make_run("pipe-b", PipelineStatus.SUCCESS, 5),
    ]
    results = compute_all_mttr(runs)
    assert len(results) == 2
    by_name = {r.pipeline: r for r in results}
    assert by_name["pipe-a"].mean_recovery_minutes == pytest.approx(15.0)
    assert by_name["pipe-b"].mean_recovery_minutes == pytest.approx(5.0)


def test_compute_all_mttr_filter_pipeline():
    runs = [
        make_run("pipe-a", PipelineStatus.FAILED, 0, error="e"),
        make_run("pipe-a", PipelineStatus.SUCCESS, 10),
        make_run("pipe-b", PipelineStatus.FAILED, 0, error="e"),
        make_run("pipe-b", PipelineStatus.SUCCESS, 5),
    ]
    results = compute_all_mttr(runs, pipeline="pipe-a")
    assert len(results) == 1
    assert results[0].pipeline == "pipe-a"


def test_mttr_str_with_data():
    r = MTTRResult(
        pipeline="pipe-a",
        total_incidents=3,
        recovered_incidents=2,
        mean_recovery_minutes=12.5,
        longest_recovery_minutes=20.0,
        shortest_recovery_minutes=5.0,
    )
    s = str(r)
    assert "pipe-a" in s
    assert "12.5" in s


def test_mttr_str_no_data():
    r = MTTRResult(
        pipeline="pipe-x",
        total_incidents=0,
        recovered_incidents=0,
        mean_recovery_minutes=None,
        longest_recovery_minutes=None,
        shortest_recovery_minutes=None,
    )
    assert "no recovery data" in str(r)
