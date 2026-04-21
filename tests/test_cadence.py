"""Tests for pipewatch.cadence."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.cadence import (
    CadenceResult,
    compute_all_cadences,
    compute_cadence,
)
from pipewatch.models import PipelineRun, PipelineStatus


def make_run(
    pipeline: str,
    started_at: datetime,
    status: PipelineStatus = PipelineStatus.SUCCESS,
) -> PipelineRun:
    return PipelineRun(
        run_id=f"{pipeline}-{started_at.isoformat()}",
        pipeline=pipeline,
        status=status,
        started_at=started_at,
        finished_at=started_at + timedelta(minutes=1),
    )


BASE = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _hourly_runs(pipeline: str, count: int, base: datetime = BASE) -> list:
    return [make_run(pipeline, base + timedelta(hours=i)) for i in range(count)]


def test_insufficient_data_fewer_than_min_runs():
    runs = _hourly_runs("pipe-a", 2)
    result = compute_cadence(runs, "pipe-a", now=BASE + timedelta(hours=3))
    assert result.status == "insufficient_data"
    assert result.expected_interval_minutes is None


def test_on_time_pipeline():
    runs = _hourly_runs("pipe-a", 5)
    # last run is at BASE+4h; now is BASE+4h+30m — well within 2x 60m window
    now = BASE + timedelta(hours=4, minutes=30)
    result = compute_cadence(runs, "pipe-a", now=now)
    assert result.status == "on_time"
    assert result.expected_interval_minutes == pytest.approx(60.0, abs=0.1)
    assert result.actual_last_gap_minutes == pytest.approx(30.0, abs=0.1)


def test_overdue_pipeline():
    runs = _hourly_runs("pipe-b", 4)
    # last run at BASE+3h; now is BASE+3h+3h — 180m gap vs 60m expected → overdue
    now = BASE + timedelta(hours=6)
    result = compute_cadence(runs, "pipe-b", now=now)
    assert result.status == "overdue"


def test_too_frequent_pipeline():
    # runs every 60 minutes historically, but last gap is only 5 minutes
    runs = _hourly_runs("pipe-c", 5)
    # inject a very recent run just 5 minutes after the last one
    last = BASE + timedelta(hours=4, minutes=5)
    runs.append(make_run("pipe-c", last))
    now = BASE + timedelta(hours=4, minutes=6)  # 1m after the latest
    result = compute_cadence(runs, "pipe-c", now=now)
    assert result.status == "too_frequent"


def test_compute_all_cadences_returns_one_per_pipeline():
    runs = _hourly_runs("alpha", 4) + _hourly_runs("beta", 4)
    now = BASE + timedelta(hours=4, minutes=30)
    results = compute_all_cadences(runs, now=now)
    pipelines = [r.pipeline for r in results]
    assert "alpha" in pipelines
    assert "beta" in pipelines
    assert len(results) == 2


def test_str_representation():
    r = CadenceResult(
        pipeline="my-pipe",
        expected_interval_minutes=60.0,
        actual_last_gap_minutes=45.0,
        run_count=5,
        status="on_time",
        note="Running within expected cadence window.",
    )
    s = str(r)
    assert "ON_TIME" in s
    assert "my-pipe" in s
    assert "60.0m" in s
