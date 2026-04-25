"""Tests for pipewatch.aging."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pipewatch.aging import AgingResult, detect_aging
from pipewatch.models import PipelineRun, PipelineStatus


NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def make_run(
    pipeline: str = "etl",
    run_id: str = "r1",
    status: PipelineStatus = PipelineStatus.FAILED,
    minutes_ago: float = 60.0,
    error: str | None = "some error",
) -> PipelineRun:
    started = NOW - timedelta(minutes=minutes_ago)
    ended = started + timedelta(minutes=1)
    return PipelineRun(
        run_id=run_id,
        pipeline=pipeline,
        status=status,
        started_at=started,
        ended_at=ended,
        error=error,
    )


@pytest.fixture(autouse=True)
def freeze_now():
    with patch("pipewatch.aging._now", return_value=NOW):
        yield


def test_detect_aging_returns_old_failures():
    runs = [make_run(minutes_ago=120)]
    results = detect_aging(runs, min_age_minutes=30)
    assert len(results) == 1
    assert results[0].pipeline == "etl"
    assert results[0].age_minutes == pytest.approx(120, abs=1)


def test_detect_aging_excludes_recent_failures():
    runs = [make_run(minutes_ago=10)]
    results = detect_aging(runs, min_age_minutes=30)
    assert results == []


def test_detect_aging_ignores_success_runs():
    runs = [make_run(status=PipelineStatus.SUCCESS, minutes_ago=500)]
    results = detect_aging(runs, min_age_minutes=30)
    assert results == []


def test_detect_aging_keeps_only_latest_failure_per_pipeline():
    runs = [
        make_run(run_id="old", minutes_ago=300),
        make_run(run_id="new", minutes_ago=60),
    ]
    results = detect_aging(runs, min_age_minutes=30)
    assert len(results) == 1
    assert results[0].run_id == "new"


def test_detect_aging_sorted_by_age_descending():
    runs = [
        make_run(pipeline="a", run_id="a1", minutes_ago=60),
        make_run(pipeline="b", run_id="b1", minutes_ago=300),
    ]
    results = detect_aging(runs, min_age_minutes=30)
    assert results[0].pipeline == "b"
    assert results[1].pipeline == "a"


def test_detect_aging_filter_by_pipeline():
    runs = [
        make_run(pipeline="a", run_id="a1", minutes_ago=120),
        make_run(pipeline="b", run_id="b1", minutes_ago=120),
    ]
    results = detect_aging(runs, min_age_minutes=30, pipeline="a")
    assert len(results) == 1
    assert results[0].pipeline == "a"


def test_severity_critical():
    r = AgingResult(
        pipeline="p", run_id="r", failed_at=NOW, age_minutes=1500, error=None
    )
    assert r.severity == "critical"


def test_severity_warning():
    r = AgingResult(
        pipeline="p", run_id="r", failed_at=NOW, age_minutes=400, error=None
    )
    assert r.severity == "warning"


def test_severity_info():
    r = AgingResult(
        pipeline="p", run_id="r", failed_at=NOW, age_minutes=45, error=None
    )
    assert r.severity == "info"


def test_str_representation():
    r = AgingResult(
        pipeline="my_pipe", run_id="abc", failed_at=NOW, age_minutes=90, error="timeout"
    )
    s = str(r)
    assert "my_pipe" in s
    assert "timeout" in s
