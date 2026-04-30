"""Tests for pipewatch.overlap."""
from __future__ import annotations

import datetime
from typing import Optional

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.overlap import detect_overlaps, OverlapResult


def make_run(
    pipeline: str,
    run_id: str,
    started_at: str,
    finished_at: Optional[str] = None,
    status: PipelineStatus = PipelineStatus.SUCCESS,
) -> PipelineRun:
    return PipelineRun(
        run_id=run_id,
        pipeline_name=pipeline,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
    )


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

T0 = "2024-01-01T10:00:00"
T1 = "2024-01-01T10:05:00"  # +5 min
T2 = "2024-01-01T10:08:00"  # +8 min
T3 = "2024-01-01T10:15:00"  # +15 min
T4 = "2024-01-01T10:20:00"  # +20 min


def test_detect_overlaps_empty():
    assert detect_overlaps([]) == []


def test_no_overlap_sequential_runs():
    runs = [
        make_run("pipe-a", "r1", T0, T1),
        make_run("pipe-b", "r2", T2, T3),
    ]
    assert detect_overlaps(runs) == []


def test_overlap_detected():
    # pipe-a: T0 -> T3, pipe-b: T1 -> T4  => overlap T1..T3 = 10 min
    runs = [
        make_run("pipe-a", "r1", T0, T3),
        make_run("pipe-b", "r2", T1, T4),
    ]
    results = detect_overlaps(runs)
    assert len(results) == 1
    r = results[0]
    assert {r.pipeline_a, r.pipeline_b} == {"pipe-a", "pipe-b"}
    assert r.overlap_seconds == pytest.approx(3 * 60.0)  # T1->T3 = 3 min


def test_same_pipeline_not_reported():
    runs = [
        make_run("pipe-a", "r1", T0, T3),
        make_run("pipe-a", "r2", T1, T4),
    ]
    assert detect_overlaps(runs) == []


def test_min_overlap_filter():
    # overlap is 3 min = 180 s; filter at 200 s should exclude it
    runs = [
        make_run("pipe-a", "r1", T0, T3),
        make_run("pipe-b", "r2", T1, T4),
    ]
    results = detect_overlaps(runs, min_overlap_seconds=200.0)
    assert results == []


def test_min_overlap_passes():
    runs = [
        make_run("pipe-a", "r1", T0, T3),
        make_run("pipe-b", "r2", T1, T4),
    ]
    results = detect_overlaps(runs, min_overlap_seconds=100.0)
    assert len(results) == 1


def test_run_without_finished_at_skipped():
    runs = [
        make_run("pipe-a", "r1", T0, None),
        make_run("pipe-b", "r2", T1, T4),
    ]
    assert detect_overlaps(runs) == []


def test_pipeline_filter():
    runs = [
        make_run("pipe-a", "r1", T0, T3),
        make_run("pipe-b", "r2", T1, T4),
        make_run("pipe-c", "r3", T0, T4),
    ]
    results = detect_overlaps(runs, pipeline="pipe-a")
    # After filtering only pipe-a runs remain, so no cross-pipeline pair
    assert results == []


def test_results_sorted_by_overlap_descending():
    # Three pipelines; create two overlapping pairs with different overlaps
    runs = [
        make_run("pipe-a", "r1", T0, T3),   # T0->T3
        make_run("pipe-b", "r2", T1, T4),   # T1->T4, overlap with a = T1..T3 = 3 min
        make_run("pipe-c", "r3", T2, T4),   # T2->T4, overlap with a = T2..T3 = 1 min
    ]
    results = detect_overlaps(runs)
    assert len(results) == 3
    assert results[0].overlap_seconds >= results[1].overlap_seconds


def test_overlap_result_str():
    r = OverlapResult(
        pipeline_a="pipe-a",
        pipeline_b="pipe-b",
        run_id_a="abcdef12",
        run_id_b="12345678",
        overlap_seconds=120.0,
        started_at_a=T0,
        started_at_b=T1,
    )
    s = str(r)
    assert "pipe-a" in s
    assert "pipe-b" in s
    assert "120.0" in s
