"""Tests for pipewatch.similarity."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.similarity import (
    SimilarityResult,
    _failure_rate,
    _error_tokens,
    compute_similarity,
    find_similar_pipelines,
)


def make_run(
    pipeline: str = "pipe",
    status: PipelineStatus = PipelineStatus.SUCCESS,
    error: str | None = None,
) -> PipelineRun:
    return PipelineRun(
        run_id="r1",
        pipeline=pipeline,
        status=status,
        started_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        ended_at=datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc),
        error=error,
    )


def test_failure_rate_all_success():
    runs = [make_run(status=PipelineStatus.SUCCESS) for _ in range(4)]
    assert _failure_rate(runs) == 0.0


def test_failure_rate_all_failed():
    runs = [make_run(status=PipelineStatus.FAILED) for _ in range(3)]
    assert _failure_rate(runs) == 1.0


def test_failure_rate_empty():
    assert _failure_rate([]) == 0.0


def test_error_tokens_extracts_words():
    runs = [
        make_run(error="connection timeout error"),
        make_run(error="timeout exceeded"),
    ]
    tokens = _error_tokens(runs)
    assert "timeout" in tokens
    assert "connection" in tokens
    assert "exceeded" in tokens


def test_error_tokens_skips_none():
    runs = [make_run(error=None), make_run(error="disk full")]
    tokens = _error_tokens(runs)
    assert "disk" in tokens
    assert "full" in tokens


def test_compute_similarity_identical_profiles():
    runs_a = [make_run(pipeline="a", status=PipelineStatus.FAILED, error="timeout error")] * 3
    runs_b = [make_run(pipeline="b", status=PipelineStatus.FAILED, error="timeout error")] * 3
    result = compute_similarity(runs_a, runs_b, "a", "b")
    assert result.score == pytest.approx(1.0, abs=0.01)
    assert result.failure_rate_delta == pytest.approx(0.0)
    assert result.shared_errors >= 2


def test_compute_similarity_different_profiles():
    runs_a = [make_run(pipeline="a", status=PipelineStatus.SUCCESS)] * 4
    runs_b = [make_run(pipeline="b", status=PipelineStatus.FAILED, error="disk full")] * 4
    result = compute_similarity(runs_a, runs_b, "a", "b")
    assert result.score < 0.5
    assert result.failure_rate_delta == pytest.approx(1.0)


def test_find_similar_pipelines_returns_above_threshold():
    runs = (
        [make_run(pipeline="alpha", status=PipelineStatus.FAILED, error="timeout")] * 3
        + [make_run(pipeline="beta", status=PipelineStatus.FAILED, error="timeout")] * 3
        + [make_run(pipeline="gamma", status=PipelineStatus.SUCCESS)] * 5
    )
    results = find_similar_pipelines(runs, threshold=0.5)
    pairs = {(r.pipeline_a, r.pipeline_b) for r in results}
    assert ("alpha", "beta") in pairs


def test_find_similar_pipelines_empty():
    assert find_similar_pipelines([], threshold=0.5) == []


def test_find_similar_pipelines_sorted_descending():
    runs = (
        [make_run(pipeline="p1", status=PipelineStatus.FAILED, error="timeout")] * 4
        + [make_run(pipeline="p2", status=PipelineStatus.FAILED, error="timeout")] * 4
        + [make_run(pipeline="p3", status=PipelineStatus.FAILED, error="disk")] * 4
    )
    results = find_similar_pipelines(runs, threshold=0.0)
    scores = [r.score for r in results]
    assert scores == sorted(scores, reverse=True)
