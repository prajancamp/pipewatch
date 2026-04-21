"""Tests for pipewatch.rca — root cause analysis."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.rca import analyze_run, analyze_all, RCAFinding


def make_run(
    status: PipelineStatus = PipelineStatus.FAILED,
    error: str | None = None,
    pipeline: str = "pipe_a",
) -> PipelineRun:
    now = datetime.now(timezone.utc).isoformat()
    return PipelineRun(
        run_id=str(uuid.uuid4()),
        pipeline=pipeline,
        status=status,
        started_at=now,
        ended_at=now,
        error=error,
    )


def test_analyze_run_success_returns_none():
    run = make_run(status=PipelineStatus.SUCCESS)
    assert analyze_run(run) is None


def test_analyze_run_failed_no_error_low_confidence():
    run = make_run(error=None)
    finding = analyze_run(run)
    assert finding is not None
    assert finding.confidence == "low"
    assert "inspect logs" in finding.causes[0].lower()


def test_analyze_run_timeout_medium_confidence():
    run = make_run(error="connection timeout after 30s")
    finding = analyze_run(run)
    assert finding is not None
    assert finding.confidence in ("medium", "high")
    assert any("timeout" in c.lower() or "connection" in c.lower() for c in finding.causes)


def test_analyze_run_permission_denied():
    run = make_run(error="PermissionError: permission denied to read table")
    finding = analyze_run(run)
    assert finding is not None
    assert any("permission" in c.lower() for c in finding.causes)


def test_analyze_run_schema_mismatch():
    run = make_run(error="schema mismatch on column user_id")
    finding = analyze_run(run)
    assert finding is not None
    assert any("schema" in c.lower() for c in finding.causes)


def test_analyze_run_multiple_patterns_high_confidence():
    run = make_run(error="null pointer: schema validation failed")
    finding = analyze_run(run)
    assert finding is not None
    assert finding.confidence == "high"
    assert len(finding.causes) >= 2


def test_analyze_run_finding_str():
    run = make_run(error="disk full")
    finding = analyze_run(run)
    assert finding is not None
    text = str(finding)
    assert "pipe_a" in text
    assert finding.confidence.upper() in text


def test_analyze_all_filters_successes():
    runs = [
        make_run(status=PipelineStatus.SUCCESS),
        make_run(status=PipelineStatus.FAILED, error="timeout"),
        make_run(status=PipelineStatus.FAILED, error="permission denied"),
    ]
    findings = analyze_all(runs)
    assert len(findings) == 2


def test_analyze_all_empty():
    assert analyze_all([]) == []


def test_finding_pipeline_and_run_id_preserved():
    run = make_run(pipeline="etl_loader", error="rate limit exceeded")
    finding = analyze_run(run)
    assert finding is not None
    assert finding.pipeline == "etl_loader"
    assert finding.run_id == run.run_id
