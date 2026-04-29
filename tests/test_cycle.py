"""Tests for pipewatch.cycle."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.cycle import _outcome_sequence, _score_period, detect_cycles


def make_run(
    pipeline: str,
    status: PipelineStatus,
    offset_seconds: int = 0,
) -> PipelineRun:
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat()
    # vary started_at so ordering is deterministic
    started = datetime(2024, 1, 1, second=offset_seconds % 60,
                       minute=offset_seconds // 60,
                       tzinfo=timezone.utc).isoformat()
    return PipelineRun(
        run_id=str(uuid.uuid4()),
        pipeline=pipeline,
        status=status,
        started_at=started,
        finished_at=ts,
    )


def _alternating(n: int, pipeline: str = "pipe") -> List[PipelineRun]:
    statuses = [PipelineStatus.SUCCESS, PipelineStatus.FAILED]
    return [make_run(pipeline, statuses[i % 2], i) for i in range(n)]


def test_outcome_sequence_ordered():
    runs = _alternating(4)
    seq = _outcome_sequence(runs)
    assert seq == ["success", "failed", "success", "failed"]


def test_score_period_perfect_alternating():
    seq = ["success", "failed"] * 6  # period 2, 12 elements
    score = _score_period(seq, 2)
    assert score == pytest.approx(1.0)


def test_score_period_too_short():
    seq = ["success", "failed", "success"]
    assert _score_period(seq, 2) == 0.0  # len//2 == 1, period 2 > 1


def test_score_period_period_one_returns_zero():
    seq = ["success"] * 10
    assert _score_period(seq, 1) == 0.0


def test_detect_cycles_finds_alternating():
    runs = _alternating(20)
    results = detect_cycles(runs, min_runs=10, min_confidence=0.75)
    assert len(results) == 1
    r = results[0]
    assert r.pipeline == "pipe"
    assert r.period == 2
    assert r.confidence >= 0.75
    assert r.pattern == ["success", "failed"]


def test_detect_cycles_insufficient_runs():
    runs = _alternating(8)  # below default min_runs=10
    results = detect_cycles(runs, min_runs=10)
    assert results == []


def test_detect_cycles_all_success_no_cycle():
    runs = [make_run("pipe", PipelineStatus.SUCCESS, i) for i in range(20)]
    results = detect_cycles(runs, min_runs=10, min_confidence=0.75)
    assert results == []


def test_detect_cycles_filter_by_pipeline():
    runs_a = _alternating(20, "alpha")
    runs_b = [make_run("beta", PipelineStatus.SUCCESS, i) for i in range(20)]
    results = detect_cycles(runs_a + runs_b, pipeline="alpha", min_runs=10)
    assert all(r.pipeline == "alpha" for r in results)


def test_detect_cycles_multiple_pipelines():
    runs_a = _alternating(20, "alpha")
    runs_b = _alternating(20, "beta")
    results = detect_cycles(runs_a + runs_b, min_runs=10)
    pipelines = {r.pipeline for r in results}
    assert "alpha" in pipelines
    assert "beta" in pipelines


def test_detect_cycles_sorted_by_confidence_desc():
    runs_a = _alternating(20, "alpha")          # perfect cycle
    # beta: mostly alternating but a few random extras lower confidence slightly
    runs_b = _alternating(20, "beta")
    runs_b[5] = make_run("beta", PipelineStatus.SUCCESS, 5)  # break pattern once
    results = detect_cycles(runs_a + runs_b, min_runs=10, min_confidence=0.5)
    if len(results) >= 2:
        assert results[0].confidence >= results[1].confidence
