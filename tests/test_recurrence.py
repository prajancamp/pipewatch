"""Tests for pipewatch.recurrence"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.recurrence import RecurrenceResult, detect_recurrence


def make_run(
    pipeline: str = "pipe",
    status: PipelineStatus = PipelineStatus.SUCCESS,
    hour: int = 10,
    day: int = 1,
) -> PipelineRun:
    started = datetime(2024, 1, day, hour, 0, 0, tzinfo=timezone.utc).isoformat()
    ended = datetime(2024, 1, day, hour, 0, 30, tzinfo=timezone.utc).isoformat()
    return PipelineRun(
        run_id=f"{pipeline}-{day}-{hour}",
        pipeline=pipeline,
        status=status,
        started_at=started,
        ended_at=ended,
    )


def test_no_results_when_below_min_occurrences():
    runs = [make_run(status=PipelineStatus.FAILED, hour=3, day=d) for d in range(1, 3)]
    results = detect_recurrence(runs, min_occurrences=3)
    assert results == []


def test_detects_recurrent_failure_slot():
    runs = [
        make_run(status=PipelineStatus.FAILED, hour=2, day=d) for d in range(1, 5)
    ]
    results = detect_recurrence(runs, min_occurrences=3, min_failure_rate=0.5)
    assert len(results) == 1
    r = results[0]
    assert r.hour_slot == 2
    assert r.failure_count == 4
    assert r.failure_rate == pytest.approx(1.0)


def test_no_flag_when_failure_rate_below_threshold():
    runs = [
        make_run(status=PipelineStatus.FAILED, hour=5, day=1),
        make_run(status=PipelineStatus.SUCCESS, hour=5, day=2),
        make_run(status=PipelineStatus.SUCCESS, hour=5, day=3),
        make_run(status=PipelineStatus.SUCCESS, hour=5, day=4),
    ]
    results = detect_recurrence(runs, min_occurrences=3, min_failure_rate=0.5)
    assert results == []


def test_filter_by_pipeline():
    runs_a = [make_run(pipeline="a", status=PipelineStatus.FAILED, hour=6, day=d) for d in range(1, 5)]
    runs_b = [make_run(pipeline="b", status=PipelineStatus.FAILED, hour=6, day=d) for d in range(1, 5)]
    results = detect_recurrence(runs_a + runs_b, min_occurrences=3, pipeline="a")
    assert all(r.pipeline == "a" for r in results)


def test_multiple_pipelines_reported():
    runs = [
        make_run(pipeline="x", status=PipelineStatus.FAILED, hour=8, day=d)
        for d in range(1, 5)
    ] + [
        make_run(pipeline="y", status=PipelineStatus.FAILED, hour=8, day=d)
        for d in range(1, 5)
    ]
    results = detect_recurrence(runs, min_occurrences=3)
    pipelines = {r.pipeline for r in results}
    assert "x" in pipelines
    assert "y" in pipelines


def test_str_representation():
    r = RecurrenceResult(
        pipeline="etl_load",
        hour_slot=14,
        failure_count=3,
        total_in_slot=4,
        failure_rate=0.75,
    )
    s = str(r)
    assert "etl_load" in s
    assert "14" in s
    assert "75%" in s


def test_sorted_by_failure_rate_descending():
    runs_high = [
        make_run(pipeline="high", status=PipelineStatus.FAILED, hour=1, day=d)
        for d in range(1, 5)
    ]
    runs_low = [
        make_run(pipeline="low", status=PipelineStatus.FAILED, hour=2, day=1),
        make_run(pipeline="low", status=PipelineStatus.SUCCESS, hour=2, day=2),
        make_run(pipeline="low", status=PipelineStatus.FAILED, hour=2, day=3),
        make_run(pipeline="low", status=PipelineStatus.SUCCESS, hour=2, day=4),
    ]
    results = detect_recurrence(runs_high + runs_low, min_occurrences=3, min_failure_rate=0.4)
    assert results[0].pipeline == "high"
