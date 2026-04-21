"""Tests for pipewatch.sla."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.sla import SLAResult, check_sla


def make_run(
    pipeline: str = "pipe",
    status: PipelineStatus = PipelineStatus.SUCCESS,
    duration: Optional[float] = 60.0,
    run_id: str = "r1",
) -> PipelineRun:
    now = datetime.now(timezone.utc).isoformat()
    return PipelineRun(
        run_id=run_id,
        pipeline=pipeline,
        status=status,
        started_at=now,
        duration_seconds=duration,
    )


def test_check_sla_all_within_threshold():
    runs = [
        make_run(duration=50.0, run_id="r1"),
        make_run(duration=100.0, run_id="r2"),
    ]
    results = check_sla(runs, threshold=200.0)
    assert len(results) == 1
    result = results[0]
    assert result.breaches == 0
    assert result.breach_rate == 0.0
    assert not result.is_breaching


def test_check_sla_detects_breach():
    runs = [
        make_run(duration=50.0, run_id="r1"),
        make_run(duration=350.0, run_id="r2"),
        make_run(duration=400.0, run_id="r3"),
    ]
    results = check_sla(runs, threshold=300.0)
    assert len(results) == 1
    r = results[0]
    assert r.breaches == 2
    assert r.total_runs == 3
    assert pytest.approx(r.breach_rate, rel=1e-3) == 2 / 3
    assert r.max_duration == 400.0
    assert r.is_breaching


def test_check_sla_multiple_pipelines():
    runs = [
        make_run(pipeline="a", duration=100.0, run_id="a1"),
        make_run(pipeline="a", duration=500.0, run_id="a2"),
        make_run(pipeline="b", duration=50.0, run_id="b1"),
    ]
    results = check_sla(runs, threshold=200.0)
    by_name = {r.pipeline: r for r in results}
    assert by_name["a"].breaches == 1
    assert by_name["b"].breaches == 0


def test_check_sla_filter_pipeline():
    runs = [
        make_run(pipeline="a", duration=500.0, run_id="a1"),
        make_run(pipeline="b", duration=500.0, run_id="b1"),
    ]
    results = check_sla(runs, threshold=200.0, pipeline="a")
    assert len(results) == 1
    assert results[0].pipeline == "a"


def test_check_sla_no_duration_excluded():
    runs = [
        make_run(duration=None, run_id="r1"),
        make_run(duration=50.0, run_id="r2"),
    ]
    results = check_sla(runs, threshold=100.0)
    assert results[0].total_runs == 1  # only the run with a duration counts


def test_check_sla_empty_runs():
    results = check_sla([], threshold=300.0)
    assert results == []


def test_sla_result_str():
    r = SLAResult(
        pipeline="etl_daily",
        total_runs=10,
        breaches=3,
        breach_rate=0.3,
        max_duration=450.0,
        threshold=300.0,
    )
    s = str(r)
    assert "etl_daily" in s
    assert "3/10" in s
    assert "30.0%" in s
    assert "300s" in s
