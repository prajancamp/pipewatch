"""Tests for pipewatch.pareto"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.pareto import compute_pareto, pareto_boundary, ParetoEntry


def make_run(
    pipeline: str,
    status: PipelineStatus = PipelineStatus.SUCCESS,
) -> PipelineRun:
    now = datetime.now(timezone.utc).isoformat()
    return PipelineRun(
        run_id=str(uuid.uuid4()),
        pipeline=pipeline,
        status=status,
        started_at=now,
        ended_at=now,
    )


def test_compute_pareto_empty():
    assert compute_pareto([]) == []


def test_compute_pareto_all_success():
    runs = [make_run("pipe-a") for _ in range(5)]
    entries = compute_pareto(runs)
    assert len(entries) == 1
    assert entries[0].failure_count == 0
    assert entries[0].cumulative_failure_pct == 0.0


def test_compute_pareto_sorted_by_failure_count():
    runs = (
        [make_run("pipe-a", PipelineStatus.FAILED)] * 5
        + [make_run("pipe-b", PipelineStatus.FAILED)] * 2
        + [make_run("pipe-c", PipelineStatus.FAILED)] * 1
    )
    entries = compute_pareto(runs)
    assert entries[0].pipeline == "pipe-a"
    assert entries[1].pipeline == "pipe-b"
    assert entries[2].pipeline == "pipe-c"


def test_compute_pareto_cumulative_pct():
    runs = (
        [make_run("pipe-a", PipelineStatus.FAILED)] * 8
        + [make_run("pipe-b", PipelineStatus.FAILED)] * 2
    )
    entries = compute_pareto(runs)
    assert entries[0].cumulative_failure_pct == pytest.approx(0.8)
    assert entries[1].cumulative_failure_pct == pytest.approx(1.0)


def test_compute_pareto_filter_pipeline():
    runs = (
        [make_run("pipe-a", PipelineStatus.FAILED)] * 3
        + [make_run("pipe-b", PipelineStatus.FAILED)] * 3
    )
    entries = compute_pareto(runs, pipeline="pipe-a")
    assert all(e.pipeline == "pipe-a" for e in entries)


def test_pareto_boundary_selects_threshold():
    entries = [
        ParetoEntry("a", 80, 100, 0.80),
        ParetoEntry("b", 15, 100, 0.95),
        ParetoEntry("c", 5, 100, 1.00),
    ]
    boundary = pareto_boundary(entries, threshold=0.8)
    assert len(boundary) == 1
    assert boundary[0].pipeline == "a"


def test_pareto_boundary_all_within_threshold():
    entries = [
        ParetoEntry("a", 50, 100, 0.50),
        ParetoEntry("b", 30, 100, 0.80),
    ]
    boundary = pareto_boundary(entries, threshold=0.9)
    assert len(boundary) == 2


def test_failure_rate():
    e = ParetoEntry("pipe", 3, 10, 0.3)
    assert e.failure_rate() == pytest.approx(0.3)


def test_str_representation():
    e = ParetoEntry("my-pipeline", 4, 10, 0.4)
    text = str(e)
    assert "my-pipeline" in text
    assert "4 failures" in text
