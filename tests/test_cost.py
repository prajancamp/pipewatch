"""Tests for pipewatch.cost module."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.cost import estimate_cost, compute_cost_summary, CostEntry, CostSummary


def make_run(
    pipeline: str = "etl",
    status: PipelineStatus = PipelineStatus.SUCCESS,
    duration: float | None = 100.0,
) -> PipelineRun:
    now = datetime.now(timezone.utc).isoformat()
    return PipelineRun(
        run_id=str(uuid.uuid4()),
        pipeline=pipeline,
        status=status,
        started_at=now,
        ended_at=now,
        duration=duration,
    )


def test_estimate_cost_with_duration():
    run = make_run(duration=200.0)
    entry = estimate_cost(run, rate_per_second=0.001)
    assert isinstance(entry, CostEntry)
    assert entry.cost_usd == pytest.approx(0.2)
    assert entry.duration_seconds == 200.0
    assert entry.pipeline == "etl"


def test_estimate_cost_no_duration():
    run = make_run(duration=None)
    entry = estimate_cost(run, rate_per_second=0.001)
    assert entry.cost_usd is None
    assert entry.duration_seconds is None


def test_estimate_cost_default_rate():
    run = make_run(duration=10000.0)
    entry = estimate_cost(run)
    assert entry.cost_usd == pytest.approx(1.0)


def test_compute_cost_summary_single_pipeline():
    runs = [make_run(duration=100.0), make_run(duration=200.0)]
    summaries = compute_cost_summary(runs, rate_per_second=0.01)
    assert "etl" in summaries
    s = summaries["etl"]
    assert s.total_runs == 2
    assert s.total_cost_usd == pytest.approx(3.0)
    assert s.avg_cost_usd == pytest.approx(1.5)
    assert s.total_duration_seconds == pytest.approx(300.0)


def test_compute_cost_summary_multiple_pipelines():
    runs = [
        make_run(pipeline="a", duration=100.0),
        make_run(pipeline="b", duration=50.0),
        make_run(pipeline="a", duration=100.0),
    ]
    summaries = compute_cost_summary(runs, rate_per_second=0.01)
    assert summaries["a"].total_runs == 2
    assert summaries["b"].total_runs == 1
    assert summaries["a"].total_cost_usd == pytest.approx(2.0)
    assert summaries["b"].total_cost_usd == pytest.approx(0.5)


def test_compute_cost_summary_empty():
    summaries = compute_cost_summary([])
    assert summaries == {}


def test_compute_cost_summary_no_durations():
    runs = [make_run(duration=None), make_run(duration=None)]
    summaries = compute_cost_summary(runs, rate_per_second=0.01)
    s = summaries["etl"]
    assert s.total_cost_usd == 0.0
    assert s.avg_cost_usd == 0.0
    assert s.total_duration_seconds == 0.0


def test_cost_entry_str():
    entry = CostEntry(pipeline="etl", run_id="abc12345", duration_seconds=60.0, cost_usd=0.006)
    text = str(entry)
    assert "etl" in text
    assert "abc1234" in text
    assert "60.0s" in text


def test_cost_summary_str():
    s = CostSummary(pipeline="etl", total_runs=5, total_cost_usd=0.05, avg_cost_usd=0.01, total_duration_seconds=500.0)
    text = str(s)
    assert "etl" in text
    assert "runs=5" in text
