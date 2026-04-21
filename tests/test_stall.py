"""Tests for pipewatch.stall."""
from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.stall import StallResult, detect_stalls


def make_run(
    pipeline: str,
    minutes_ago: float,
    status: PipelineStatus = PipelineStatus.SUCCESS,
    run_id: str | None = None,
) -> PipelineRun:
    now = datetime.now(timezone.utc)
    started = now - timedelta(minutes=minutes_ago)
    ended = started + timedelta(minutes=1)
    return PipelineRun(
        run_id=run_id or f"{pipeline}-{minutes_ago}",
        pipeline=pipeline,
        status=status,
        started_at=started,
        ended_at=ended,
    )


NOW = datetime.now(timezone.utc)


def test_no_stall_when_recent():
    runs = [make_run("etl", minutes_ago=5)]
    results = detect_stalls(runs, expected_interval_minutes=60, now=NOW)
    assert len(results) == 1
    assert results[0].is_stalled is False
    assert results[0].pipeline == "etl"


def test_stall_when_overdue():
    runs = [make_run("etl", minutes_ago=90)]
    results = detect_stalls(runs, expected_interval_minutes=60, now=NOW)
    assert len(results) == 1
    assert results[0].is_stalled is True
    assert results[0].minutes_since_last_run > 60


def test_only_latest_run_checked():
    """Older runs should not mask a stall."""
    runs = [
        make_run("etl", minutes_ago=5, run_id="recent"),
        make_run("etl", minutes_ago=120, run_id="old"),
    ]
    results = detect_stalls(runs, expected_interval_minutes=60, now=NOW)
    assert len(results) == 1
    assert results[0].is_stalled is False  # most recent was 5m ago


def test_multiple_pipelines_independent():
    runs = [
        make_run("pipe-a", minutes_ago=10),
        make_run("pipe-b", minutes_ago=120),
    ]
    results = detect_stalls(runs, expected_interval_minutes=60, now=NOW)
    by_name = {r.pipeline: r for r in results}
    assert by_name["pipe-a"].is_stalled is False
    assert by_name["pipe-b"].is_stalled is True


def test_filter_by_pipeline():
    runs = [
        make_run("pipe-a", minutes_ago=10),
        make_run("pipe-b", minutes_ago=120),
    ]
    results = detect_stalls(runs, expected_interval_minutes=60, pipeline="pipe-b", now=NOW)
    assert len(results) == 1
    assert results[0].pipeline == "pipe-b"


def test_empty_runs_returns_empty():
    results = detect_stalls([], expected_interval_minutes=30, now=NOW)
    assert results == []


def test_str_stalled():
    runs = [make_run("slow-pipe", minutes_ago=90)]
    result = detect_stalls(runs, expected_interval_minutes=60, now=NOW)[0]
    text = str(result)
    assert "STALLED" in text
    assert "slow-pipe" in text


def test_str_ok():
    runs = [make_run("fast-pipe", minutes_ago=5)]
    result = detect_stalls(runs, expected_interval_minutes=60, now=NOW)[0]
    text = str(result)
    assert "OK" in text
    assert "fast-pipe" in text
