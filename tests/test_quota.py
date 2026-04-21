"""Tests for pipewatch.quota."""
from __future__ import annotations

import datetime
from typing import Optional, List

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.quota import check_quota, breaching_quotas, QuotaResult


def make_run(
    pipeline: str,
    hours_ago: float = 1.0,
    status: PipelineStatus = PipelineStatus.SUCCESS,
) -> PipelineRun:
    now = datetime.datetime.utcnow()
    started = now - datetime.timedelta(hours=hours_ago)
    ended = started + datetime.timedelta(minutes=5)
    return PipelineRun(
        run_id=f"{pipeline}-{hours_ago}",
        pipeline=pipeline,
        status=status,
        started_at=started.isoformat(),
        ended_at=ended.isoformat(),
    )


def test_check_quota_no_breach():
    runs = [make_run("etl", hours_ago=i) for i in range(1, 4)]
    results = check_quota(runs, expected_max=10, window_hours=24)
    assert len(results) == 1
    assert results[0].pipeline == "etl"
    assert results[0].actual_count == 3
    assert not results[0].breaching


def test_check_quota_breach():
    runs = [make_run("etl", hours_ago=i * 0.1) for i in range(1, 6)]
    results = check_quota(runs, expected_max=3, window_hours=24)
    assert results[0].breaching
    assert results[0].actual_count == 5


def test_check_quota_excludes_old_runs():
    recent = [make_run("etl", hours_ago=1)]
    old = [make_run("etl", hours_ago=30)]
    results = check_quota(recent + old, expected_max=1, window_hours=24)
    # Only 1 recent run, not breaching max=1
    assert results[0].actual_count == 1
    assert not results[0].breaching


def test_check_quota_filter_pipeline():
    runs = [
        make_run("alpha", hours_ago=1),
        make_run("beta", hours_ago=1),
        make_run("beta", hours_ago=2),
    ]
    results = check_quota(runs, expected_max=10, window_hours=24, pipeline="beta")
    assert len(results) == 1
    assert results[0].pipeline == "beta"
    assert results[0].actual_count == 2


def test_check_quota_multiple_pipelines():
    runs = [
        make_run("alpha", hours_ago=1),
        make_run("beta", hours_ago=1),
        make_run("beta", hours_ago=2),
    ]
    results = check_quota(runs, expected_max=1, window_hours=24)
    by_name = {r.pipeline: r for r in results}
    assert not by_name["alpha"].breaching
    assert by_name["beta"].breaching


def test_breaching_quotas_filters():
    results = [
        QuotaResult("ok", 10, 3, False, 24),
        QuotaResult("bad", 10, 15, True, 24),
    ]
    breaching = breaching_quotas(results)
    assert len(breaching) == 1
    assert breaching[0].pipeline == "bad"


def test_quota_result_str_breach():
    r = QuotaResult("my_pipeline", 5, 9, True, 24)
    assert "BREACH" in str(r)
    assert "my_pipeline" in str(r)


def test_quota_result_str_ok():
    r = QuotaResult("my_pipeline", 5, 2, False, 24)
    assert "OK" in str(r)


def test_check_quota_empty_runs():
    results = check_quota([], expected_max=10, window_hours=24)
    assert results == []
