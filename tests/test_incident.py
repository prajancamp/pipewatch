"""Tests for pipewatch.incident."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.incident import detect_incidents, Incident


def make_run(
    pipeline: str = "pipe",
    status: str = "failed",
    offset_minutes: int = 0,
    error: str | None = "err",
) -> PipelineRun:
    start = datetime(2024, 1, 1, 12, 0, 0) + timedelta(minutes=offset_minutes)
    end = start + timedelta(minutes=1)
    return PipelineRun(
        run_id=str(uuid.uuid4()),
        pipeline=pipeline,
        status=PipelineStatus(status),
        started_at=start.isoformat(),
        finished_at=end.isoformat(),
        error=error if status == "failed" else None,
    )


def test_detect_incidents_empty():
    assert detect_incidents([]) == []


def test_no_incident_all_success():
    runs = [make_run(status="success", offset_minutes=i) for i in range(5)]
    assert detect_incidents(runs) == []


def test_single_failure_below_min_length():
    runs = [make_run(status="failed", offset_minutes=0)]
    assert detect_incidents(runs, min_length=2) == []


def test_detects_incident_two_consecutive_failures():
    runs = [
        make_run(status="failed", offset_minutes=0),
        make_run(status="failed", offset_minutes=1),
    ]
    incidents = detect_incidents(runs, min_length=2)
    assert len(incidents) == 1
    assert incidents[0].pipeline == "pipe"
    assert incidents[0].length == 2


def test_incident_resets_on_success():
    runs = [
        make_run(status="failed", offset_minutes=0),
        make_run(status="failed", offset_minutes=1),
        make_run(status="success", offset_minutes=2),
        make_run(status="failed", offset_minutes=3),
        make_run(status="failed", offset_minutes=4),
        make_run(status="failed", offset_minutes=5),
    ]
    incidents = detect_incidents(runs, min_length=2)
    assert len(incidents) == 2
    assert incidents[1].length == 3


def test_incident_str():
    runs = [
        make_run(status="failed", offset_minutes=0),
        make_run(status="failed", offset_minutes=1),
    ]
    inc = detect_incidents(runs)[0]
    s = str(inc)
    assert "pipe" in s
    assert "2 failures" in s


def test_multiple_pipelines_independent():
    runs = [
        make_run(pipeline="a", status="failed", offset_minutes=0),
        make_run(pipeline="a", status="failed", offset_minutes=1),
        make_run(pipeline="b", status="success", offset_minutes=0),
        make_run(pipeline="b", status="failed", offset_minutes=1),
    ]
    incidents = detect_incidents(runs, min_length=2)
    assert len(incidents) == 1
    assert incidents[0].pipeline == "a"


def test_incident_errors_collected():
    runs = [
        make_run(status="failed", offset_minutes=0, error="timeout"),
        make_run(status="failed", offset_minutes=1, error="connection refused"),
    ]
    inc = detect_incidents(runs)[0]
    assert "timeout" in inc.errors
    assert "connection refused" in inc.errors


def test_min_length_one_accepts_single_failure():
    runs = [make_run(status="failed", offset_minutes=0)]
    incidents = detect_incidents(runs, min_length=1)
    assert len(incidents) == 1
