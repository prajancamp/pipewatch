"""Tests for pipewatch.watchdog."""
import pytest
from datetime import datetime, timezone, timedelta
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.watchdog import find_stale_pipelines, StaleAlert


def make_run(pipeline: str, minutes_ago: float, status=PipelineStatus.SUCCESS) -> PipelineRun:
    started = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    finished = started + timedelta(seconds=10)
    return PipelineRun(
        run_id=f"{pipeline}-{minutes_ago}",
        pipeline=pipeline,
        status=status,
        started_at=started,
        finished_at=finished,
    )


def test_no_stale_when_recent():
    runs = [make_run("etl", 5)]
    alerts = find_stale_pipelines(runs, stale_after_minutes=60)
    assert alerts == []


def test_stale_when_old():
    runs = [make_run("etl", 120)]
    alerts = find_stale_pipelines(runs, stale_after_minutes=60)
    assert len(alerts) == 1
    assert alerts[0].pipeline == "etl"


def test_only_latest_run_checked():
    runs = [
        make_run("etl", 200),
        make_run("etl", 10),
    ]
    alerts = find_stale_pipelines(runs, stale_after_minutes=60)
    assert alerts == []


def test_multiple_pipelines_partial_stale():
    runs = [
        make_run("etl", 10),
        make_run("load", 90),
    ]
    alerts = find_stale_pipelines(runs, stale_after_minutes=60)
    assert len(alerts) == 1
    assert alerts[0].pipeline == "load"


def test_per_pipeline_threshold():
    runs = [
        make_run("fast", 20),
        make_run("slow", 20),
    ]
    alerts = find_stale_pipelines(
        runs,
        stale_after_minutes=60,
        pipeline_thresholds={"fast": 10},
    )
    assert len(alerts) == 1
    assert alerts[0].pipeline == "fast"


def test_stale_alert_str():
    run = make_run("etl", 90)
    alerts = find_stale_pipelines([run], stale_after_minutes=60)
    assert "STALE" in str(alerts[0])
    assert "etl" in str(alerts[0])


def test_empty_runs():
    assert find_stale_pipelines([], stale_after_minutes=30) == []
