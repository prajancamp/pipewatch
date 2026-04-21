"""Tests for pipewatch.streak"""

import pytest
from datetime import datetime, timezone
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.streak import compute_streaks, _compute_streak


def make_run(pipeline: str, status: str, offset: int = 0) -> PipelineRun:
    start = datetime(2024, 1, 1, 12, offset, 0, tzinfo=timezone.utc)
    end = datetime(2024, 1, 1, 12, offset, 30, tzinfo=timezone.utc)
    return PipelineRun(
        run_id=f"{pipeline}-{offset}",
        pipeline=pipeline,
        status=PipelineStatus(status),
        started_at=start,
        ended_at=end,
    )


def test_compute_streaks_empty():
    result = compute_streaks([])
    assert result == {}


def test_current_success_streak():
    runs = [
        make_run("etl", "success", 0),
        make_run("etl", "success", 1),
        make_run("etl", "success", 2),
    ]
    result = compute_streaks(runs)
    assert "etl" in result
    r = result["etl"]
    assert r.current_streak_type == "success"
    assert r.current_streak_length == 3
    assert r.longest_success_streak == 3
    assert r.longest_failure_streak == 0


def test_current_failure_streak():
    runs = [
        make_run("etl", "success", 0),
        make_run("etl", "failed", 1),
        make_run("etl", "failed", 2),
    ]
    result = compute_streaks(runs)
    r = result["etl"]
    assert r.current_streak_type == "failure"
    assert r.current_streak_length == 2
    assert r.longest_success_streak == 1
    assert r.longest_failure_streak == 2


def test_longest_success_streak_in_middle():
    runs = [
        make_run("pipe", "success", 0),
        make_run("pipe", "success", 1),
        make_run("pipe", "success", 2),
        make_run("pipe", "failed", 3),
        make_run("pipe", "success", 4),
    ]
    result = compute_streaks(runs)
    r = result["pipe"]
    assert r.longest_success_streak == 3
    assert r.current_streak_type == "success"
    assert r.current_streak_length == 1


def test_multiple_pipelines_isolated():
    runs = [
        make_run("a", "success", 0),
        make_run("a", "success", 1),
        make_run("b", "failed", 0),
        make_run("b", "failed", 1),
        make_run("b", "failed", 2),
    ]
    result = compute_streaks(runs)
    assert result["a"].current_streak_type == "success"
    assert result["a"].current_streak_length == 2
    assert result["b"].current_streak_type == "failure"
    assert result["b"].current_streak_length == 3


def test_str_representation():
    runs = [make_run("mypipe", "success", i) for i in range(3)]
    result = compute_streaks(runs)
    s = str(result["mypipe"])
    assert "mypipe" in s
    assert "success" in s
    assert "3" in s
