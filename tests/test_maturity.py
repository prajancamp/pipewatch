"""Tests for pipewatch.maturity."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.maturity import (
    MaturityResult,
    _level,
    score_maturity,
    build_maturity_report,
)


def make_run(
    pipeline: str = "pipe",
    status: PipelineStatus = PipelineStatus.SUCCESS,
    run_id: str = "r1",
) -> PipelineRun:
    return PipelineRun(
        run_id=run_id,
        pipeline=pipeline,
        status=status,
        started_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        finished_at=datetime(2024, 1, 1, 12, 0, 30, tzinfo=timezone.utc),
    )


def test_level_boundaries():
    assert _level(95.0) == "veteran"
    assert _level(80.0) == "mature"
    assert _level(60.0) == "stable"
    assert _level(40.0) == "developing"
    assert _level(10.0) == "infant"


def test_score_maturity_returns_none_for_unknown_pipeline():
    runs = [make_run(pipeline="alpha", run_id="r1")]
    result = score_maturity("beta", runs)
    assert result is None


def test_score_maturity_all_success():
    runs = [make_run(run_id=f"r{i}") for i in range(50)]
    result = score_maturity("pipe", runs)
    assert result is not None
    assert result.success_rate == 1.0
    assert result.score > 70
    assert result.level in ("mature", "veteran", "stable")


def test_score_maturity_all_failures():
    runs = [
        make_run(run}", status=PipelineStatus.FAILED) for i in range(10)
    ]
    result = score_maturity("pipe", runs)
    assert result is not None
    assert result.success_rate == 0. < 35
    assert result.level in ("infant", "developing")


def test_score_maturity_single_run():
    runs = [make_run(run_id="only")]
    result = score_maturity("pipe", runs)
    assert result is not None
    assert result.total_runs == 1


def test_build_maturity_report_multiple_pipelines():
    runs: List[PipelineRun] = [
        make_run(pipeline="alpha", run_id="a1"),
        make_run(pipeline="alpha", run_id="a2"),
        make_run(pipeline="beta", run_id="b1", status=PipelineStatus.FAILED),
    ]
    report = build_maturity_report(runs)
    pipelines = [r.pipeline for r in report]
    assert "alpha" in pipelines
    assert "beta" in pipelines


def test_build_maturity_report_empty():
    assert build_maturity_report([]) == []


def test_maturity_str_contains_pipeline_name():
    runs = [make_run(run_id=f"r{i}") for i in range(5)]
    result = score_maturity("pipe", runs)
    assert result is not None
    text = str(result)
    assert "pipe" in text
    assert "score=" in text
