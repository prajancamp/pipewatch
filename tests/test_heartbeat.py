"""Tests for pipewatch.heartbeat."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pipewatch.heartbeat import (
    HeartbeatResult,
    check_all_heartbeats,
    check_heartbeat,
)
from pipewatch.models import PipelineRun, PipelineStatus

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def make_run(
    pipeline: str = "etl",
    minutes_ago: float = 10.0,
    status: PipelineStatus = PipelineStatus.SUCCESS,
) -> PipelineRun:
    started = _NOW - timedelta(minutes=minutes_ago)
    ended = started + timedelta(minutes=1)
    return PipelineRun(
        run_id=f"{pipeline}-{minutes_ago}",
        pipeline=pipeline,
        status=status,
        started_at=started,
        ended_at=ended,
    )


@pytest.fixture(autouse=True)
def freeze_now():
    with patch("pipewatch.heartbeat._now", return_value=_NOW):
        yield


def test_no_runs_for_pipeline_is_dead():
    result = check_heartbeat([], "etl", expected_interval_minutes=60.0)
    assert result.is_dead is True
    assert result.last_seen is None
    assert result.silence_minutes is None


def test_recent_run_is_alive():
    runs = [make_run(minutes_ago=30.0)]
    result = check_heartbeat(runs, "etl", expected_interval_minutes=60.0, grace_factor=2.0)
    # threshold = 120 min; silence = 30 min → alive
    assert result.is_dead is False
    assert result.silence_minutes == pytest.approx(30.0, abs=0.01)


def test_old_run_is_dead():
    runs = [make_run(minutes_ago=200.0)]
    result = check_heartbeat(runs, "etl", expected_interval_minutes=60.0, grace_factor=2.0)
    # threshold = 120 min; silence = 200 min → dead
    assert result.is_dead is True


def test_uses_most_recent_run_only():
    runs = [
        make_run(minutes_ago=200.0),
        make_run(minutes_ago=10.0),
    ]
    result = check_heartbeat(runs, "etl", expected_interval_minutes=60.0, grace_factor=2.0)
    assert result.is_dead is False
    assert result.silence_minutes == pytest.approx(10.0, abs=0.01)


def test_check_all_heartbeats_covers_all_pipelines():
    runs = [
        make_run(pipeline="a", minutes_ago=10.0),
        make_run(pipeline="b", minutes_ago=300.0),
    ]
    results = check_all_heartbeats(runs, expected_interval_minutes=60.0, grace_factor=2.0)
    by_name = {r.pipeline: r for r in results}
    assert "a" in by_name
    assert "b" in by_name
    assert by_name["a"].is_dead is False
    assert by_name["b"].is_dead is True


def test_str_never_seen():
    r = HeartbeatResult(
        pipeline="etl",
        last_seen=None,
        silence_minutes=None,
        expected_interval_minutes=60.0,
        is_dead=True,
    )
    assert "NEVER SEEN" in str(r)
    assert "etl" in str(r)


def test_str_dead_pipeline():
    r = HeartbeatResult(
        pipeline="etl",
        last_seen=_NOW - timedelta(minutes=200),
        silence_minutes=200.0,
        expected_interval_minutes=60.0,
        is_dead=True,
    )
    assert "DEAD" in str(r)


def test_str_ok_pipeline():
    r = HeartbeatResult(
        pipeline="etl",
        last_seen=_NOW - timedelta(minutes=10),
        silence_minutes=10.0,
        expected_interval_minutes=60.0,
        is_dead=False,
    )
    assert "OK" in str(r)
