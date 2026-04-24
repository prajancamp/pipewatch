"""Tests for pipewatch.flap — flap detection."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.flap import FlapResult, _count_transitions, detect_flaps
from pipewatch.models import PipelineRun, PipelineStatus


def make_run(
    pipeline: str,
    status: str,
    started_at: str = "2024-01-01T00:00:00",
) -> PipelineRun:
    return PipelineRun(
        run_id=f"{pipeline}-{status}-{started_at}",
        pipeline=pipeline,
        status=PipelineStatus(status),
        started_at=started_at,
        ended_at=started_at,
    )


def _runs(pipeline: str, pattern: List[str], base_hour: int = 0) -> List[PipelineRun]:
    """Create runs from a status pattern list."""
    runs = []
    for i, s in enumerate(pattern):
        ts = f"2024-01-01T{(base_hour + i):02d}:00:00"
        runs.append(make_run(pipeline, s, started_at=ts))
    return runs


# ── _count_transitions ────────────────────────────────────────────────────────

def test_count_transitions_no_flips():
    assert _count_transitions(["success", "success", "success"]) == 0


def test_count_transitions_all_flips():
    assert _count_transitions(["success", "failed", "success", "failed"]) == 3


def test_count_transitions_single():
    assert _count_transitions(["success"]) == 0


def test_count_transitions_empty():
    assert _count_transitions([]) == 0


# ── detect_flaps ──────────────────────────────────────────────────────────────

def test_detect_flaps_empty_returns_empty():
    assert detect_flaps([]) == []


def test_detect_flaps_below_min_runs_skipped():
    runs = _runs("pipe-a", ["success", "failed", "success"])  # only 3 < default 4
    results = detect_flaps(runs, min_runs=4)
    assert results == []


def test_detect_flaps_stable_pipeline():
    runs = _runs("pipe-a", ["success", "success", "success", "success"])
    results = detect_flaps(runs, min_runs=4, flap_threshold=0.5)
    assert len(results) == 1
    r = results[0]
    assert r.is_flapping is False
    assert r.transitions == 0
    assert r.flap_rate == 0.0


def test_detect_flaps_flapping_pipeline():
    runs = _runs("pipe-b", ["success", "failed", "success", "failed", "success"])
    results = detect_flaps(runs, min_runs=4, flap_threshold=0.5)
    assert len(results) == 1
    r = results[0]
    assert r.is_flapping is True
    assert r.transitions == 4
    assert r.flap_rate == pytest.approx(1.0)


def test_detect_flaps_sorted_by_flap_rate_desc():
    stable = _runs("stable", ["success", "success", "success", "success"])
    flappy = _runs("flappy", ["success", "failed", "success", "failed"])
    results = detect_flaps(stable + flappy, min_runs=4)
    assert results[0].pipeline == "flappy"
    assert results[1].pipeline == "stable"


def test_detect_flaps_filter_by_pipeline():
    runs_a = _runs("pipe-a", ["success", "failed", "success", "failed"])
    runs_b = _runs("pipe-b", ["success", "success", "success", "success"])
    results = detect_flaps(runs_a + runs_b, pipeline="pipe-a", min_runs=4)
    assert len(results) == 1
    assert results[0].pipeline == "pipe-a"


def test_flap_result_str_contains_flag():
    r = FlapResult(
        pipeline="my-pipe",
        total_runs=6,
        transitions=4,
        flap_rate=0.8,
        is_flapping=True,
        last_statuses=["success", "failed", "success", "failed", "success", "failed"],
    )
    s = str(r)
    assert "[FLAPPING]" in s
    assert "my-pipe" in s


def test_flap_result_str_stable():
    r = FlapResult(
        pipeline="good-pipe",
        total_runs=4,
        transitions=0,
        flap_rate=0.0,
        is_flapping=False,
        last_statuses=["success", "success", "success", "success"],
    )
    assert "[stable]" in str(r)
