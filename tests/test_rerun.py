"""Tests for pipewatch.rerun suggest_reruns."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.rerun import suggest_reruns, RerunCandidate, _is_transient_error


def make_run(
    pipeline: str,
    status: PipelineStatus = PipelineStatus.SUCCESS,
    error: Optional[str] = None,
    started: Optional[datetime] = None,
) -> PipelineRun:
    ts = started or datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc)
    return PipelineRun(
        run_id=f"{pipeline}-{ts.timestamp()}",
        pipeline=pipeline,
        status=status,
        started_at=ts,
        finished_at=ts.replace(second=ts.second + 5),
        error=error,
    )


def test_is_transient_error_timeout():
    assert _is_transient_error("Connection timeout after 30s") is True


def test_is_transient_error_rate_limit():
    assert _is_transient_error("rate limit exceeded") is True


def test_is_transient_error_none():
    assert _is_transient_error(None) is False


def test_is_transient_error_permanent():
    assert _is_transient_error("KeyError: 'missing_column'") is False


def test_suggest_reruns_empty():
    assert suggest_reruns([]) == []


def test_suggest_reruns_no_failures():
    runs = [make_run("pipe_a"), make_run("pipe_b")]
    assert suggest_reruns(runs) == []


def test_suggest_reruns_isolated_failure():
    runs = [
        make_run("pipe_a", PipelineStatus.SUCCESS),
        make_run("pipe_a", PipelineStatus.SUCCESS),
        make_run("pipe_a", PipelineStatus.FAILED, error="some error"),
    ]
    result = suggest_reruns(runs)
    assert len(result) == 1
    assert result[0].pipeline == "pipe_a"
    assert result[0].reason == "historically_reliable"


def test_suggest_reruns_transient_error():
    runs = [
        make_run("pipe_b", PipelineStatus.FAILED, error="network timeout"),
        make_run("pipe_b", PipelineStatus.FAILED, error="network timeout"),
    ]
    result = suggest_reruns(runs)
    assert len(result) == 1
    assert result[0].reason == "transient_error"


def test_suggest_reruns_skips_chronic_failures():
    # 5 consecutive failures, low success rate => skip
    runs = [make_run("bad_pipe", PipelineStatus.FAILED, error="permanent bug")] * 6
    result = suggest_reruns(runs, max_consecutive=3, min_success_rate=0.5)
    assert result == []


def test_suggest_reruns_sorted_by_success_rate():
    runs_a = [
        make_run("pipe_a", PipelineStatus.SUCCESS),
        make_run("pipe_a", PipelineStatus.SUCCESS),
        make_run("pipe_a", PipelineStatus.FAILED, error="timeout"),
    ]
    runs_b = [
        make_run("pipe_b", PipelineStatus.SUCCESS),
        make_run("pipe_b", PipelineStatus.FAILED, error="timeout"),
    ]
    result = suggest_reruns(runs_a + runs_b)
    assert result[0].pipeline == "pipe_a"  # higher success rate first
    assert result[1].pipeline == "pipe_b"


def test_rerun_candidate_str():
    c = RerunCandidate(
        pipeline="my_pipe",
        last_error="timeout",
        consecutive_failures=1,
        success_rate=0.9,
        reason="transient_error",
    )
    s = str(c)
    assert "my_pipe" in s
    assert "90.0%" in s
    assert "transient_error" in s
