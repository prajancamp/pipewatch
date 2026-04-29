"""Tests for pipewatch.quorum."""
from __future__ import annotations

import pytest
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.quorum import check_quorum, QuorumResult


def make_run(
    pipeline: str = "pipe",
    status: str = "success",
    started_at: str = "2024-01-01T00:00:00",
) -> PipelineRun:
    return PipelineRun(
        run_id=f"{pipeline}-{started_at}-{status}",
        pipeline=pipeline,
        status=PipelineStatus(status),
        started_at=started_at,
        ended_at=started_at,
    )


def test_quorum_all_success():
    runs = [make_run(status="success", started_at=f"2024-01-0{i}T00:00:00") for i in range(1, 6)]
    results = check_quorum(runs, window=5, required=3)
    assert len(results) == 1
    r = results[0]
    assert r.quorum_status == PipelineStatus.SUCCESS
    assert r.confident is True
    assert r.success_votes == 5
    assert r.failure_votes == 0


def test_quorum_majority_failures():
    runs = (
        [make_run(status="failed", started_at=f"2024-01-0{i}T00:00:00") for i in range(1, 4)]
        + [make_run(status="success", started_at=f"2024-01-0{i}T00:00:00") for i in range(4, 6)]
    )
    results = check_quorum(runs, window=5, required=3)
    r = results[0]
    assert r.quorum_status == PipelineStatus.FAILED
    assert r.confident is True
    assert r.failure_votes == 3


def test_quorum_undecided():
    runs = (
        [make_run(status="failed", started_at=f"2024-01-0{i}T00:00:00") for i in range(1, 3)]
        + [make_run(status="success", started_at=f"2024-01-0{i}T00:00:00") for i in range(3, 5)]
    )
    results = check_quorum(runs, window=4, required=3)
    r = results[0]
    assert r.quorum_status is None
    assert r.confident is False


def test_quorum_window_limits_runs():
    # 10 runs total but window=3; only last 3 matter
    runs = (
        [make_run(status="failed", started_at=f"2024-01-{i:02d}T00:00:00") for i in range(1, 8)]
        + [make_run(status="success", started_at=f"2024-01-{i:02d}T00:00:00") for i in range(8, 11)]
    )
    results = check_quorum(runs, window=3, required=2)
    r = results[0]
    # last 3 are all success
    assert r.quorum_status == PipelineStatus.SUCCESS
    assert r.window == 3


def test_quorum_filter_pipeline():
    runs = [
        make_run(pipeline="alpha", status="success", started_at="2024-01-01T00:00:00"),
        make_run(pipeline="alpha", status="success", started_at="2024-01-02T00:00:00"),
        make_run(pipeline="alpha", status="success", started_at="2024-01-03T00:00:00"),
        make_run(pipeline="beta", status="failed", started_at="2024-01-01T00:00:00"),
        make_run(pipeline="beta", status="failed", started_at="2024-01-02T00:00:00"),
        make_run(pipeline="beta", status="failed", started_at="2024-01-03T00:00:00"),
    ]
    results = check_quorum(runs, pipeline="alpha", window=5, required=3)
    assert len(results) == 1
    assert results[0].pipeline == "alpha"
    assert results[0].quorum_status == PipelineStatus.SUCCESS


def test_quorum_required_exceeds_window_raises():
    runs = [make_run()]
    with pytest.raises(ValueError, match="required cannot exceed window"):
        check_quorum(runs, window=3, required=5)


def test_quorum_empty_runs():
    results = check_quorum([], window=5, required=3)
    assert results == []


def test_quorum_multiple_pipelines_sorted():
    runs = [
        make_run(pipeline="z_pipe", status="success", started_at="2024-01-01T00:00:00"),
        make_run(pipeline="z_pipe", status="success", started_at="2024-01-02T00:00:00"),
        make_run(pipeline="z_pipe", status="success", started_at="2024-01-03T00:00:00"),
        make_run(pipeline="a_pipe", status="failed", started_at="2024-01-01T00:00:00"),
        make_run(pipeline="a_pipe", status="failed", started_at="2024-01-02T00:00:00"),
        make_run(pipeline="a_pipe", status="failed", started_at="2024-01-03T00:00:00"),
    ]
    results = check_quorum(runs, window=5, required=3)
    assert results[0].pipeline == "a_pipe"
    assert results[1].pipeline == "z_pipe"
