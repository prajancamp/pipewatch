"""Tests for pipewatch.lifespan"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pipewatch.lifespan import (
    LifespanResult,
    compute_all_lifespans,
    compute_lifespan,
)
from pipewatch.models import PipelineRun, PipelineStatus

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def make_run(
    pipeline: str,
    days_ago: float,
    status: PipelineStatus = PipelineStatus.SUCCESS,
) -> PipelineRun:
    started = _NOW - timedelta(days=days_ago)
    ended = started + timedelta(minutes=5)
    return PipelineRun(
        run_id=f"{pipeline}-{days_ago}",
        pipeline=pipeline,
        status=status,
        started_at=started,
        ended_at=ended,
    )


@pytest.fixture(autouse=True)
def freeze_now():
    with patch("pipewatch.lifespan._now", return_value=_NOW):
        yield


def test_compute_lifespan_no_runs():
    result = compute_lifespan([], "pipe-a")
    assert result is None


def test_compute_lifespan_single_run():
    runs = [make_run("pipe-a", days_ago=10)]
    result = compute_lifespan(runs, "pipe-a")
    assert result is not None
    assert result.pipeline == "pipe-a"
    assert result.total_runs == 1
    assert abs(result.age_days - 10.0) < 0.1
    assert result.warning is None


def test_compute_lifespan_multiple_runs():
    runs = [
        make_run("pipe-a", days_ago=200),
        make_run("pipe-a", days_ago=100),
        make_run("pipe-a", days_ago=5),
    ]
    result = compute_lifespan(runs, "pipe-a")
    assert result is not None
    assert result.total_runs == 3
    assert abs(result.age_days - 200.0) < 0.1


def test_compute_lifespan_warns_when_old():
    runs = [make_run("pipe-a", days_ago=200)]
    result = compute_lifespan(runs, "pipe-a", warn_after_days=180.0)
    assert result is not None
    assert result.warning is not None
    assert "200" in result.warning or "review" in result.warning


def test_compute_lifespan_no_warning_when_young():
    runs = [make_run("pipe-a", days_ago=30)]
    result = compute_lifespan(runs, "pipe-a", warn_after_days=180.0)
    assert result is not None
    assert result.warning is None


def test_compute_all_lifespans_multiple_pipelines():
    runs = [
        make_run("alpha", days_ago=50),
        make_run("beta", days_ago=10),
        make_run("alpha", days_ago=5),
    ]
    results = compute_all_lifespans(runs)
    pipelines = [r.pipeline for r in results]
    assert "alpha" in pipelines
    assert "beta" in pipelines
    alpha = next(r for r in results if r.pipeline == "alpha")
    assert alpha.total_runs == 2


def test_compute_all_lifespans_empty():
    results = compute_all_lifespans([])
    assert results == []


def test_lifespan_str_includes_pipeline():
    runs = [make_run("my-pipe", days_ago=20)]
    result = compute_lifespan(runs, "my-pipe")
    assert result is not None
    text = str(result)
    assert "my-pipe" in text
    assert "runs=1" in text
