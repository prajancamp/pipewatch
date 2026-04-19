"""Tests for pipewatch.cluster."""
from __future__ import annotations
import pytest
from datetime import datetime, timezone
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.cluster import cluster_by_error, _normalize_error


def make_run(pipeline="pipe", status=PipelineStatus.FAILED, error=None):
    now = datetime.now(timezone.utc).isoformat()
    return PipelineRun(
        run_id="r1",
        pipeline=pipeline,
        status=status,
        started_at=now,
        ended_at=now,
        error=error,
    )


def test_normalize_strips_numbers():
    assert "<n>" in _normalize_error("timeout after 30 seconds")


def test_normalize_strips_uuid():
    key = _normalize_error("failed job 3f6c1a2b-1234-5678-abcd-ef0123456789")
    assert "<uuid>" in key


def test_normalize_strips_path():
    key = _normalize_error("file /var/data/input.csv not found")
    assert "<path>" in key


def test_cluster_groups_similar_errors():
    runs = [
        make_run(pipeline="a", error="timeout after 30 seconds"),
        make_run(pipeline="b", error="timeout after 60 seconds"),
        make_run(pipeline="c", error="connection refused"),
    ]
    clusters = cluster_by_error(runs)
    # the two timeout errors should share a cluster
    timeout_cluster = next((c for c in clusters if "timeout" in c.key), None)
    assert timeout_cluster is not None
    assert timeout_cluster.count == 2


def test_cluster_skips_successful_runs():
    runs = [
        make_run(status=PipelineStatus.SUCCESS, error=None),
        make_run(status=PipelineStatus.FAILED, error="disk full"),
    ]
    clusters = cluster_by_error(runs)
    assert sum(c.count for c in clusters) == 1


def test_cluster_empty_returns_empty():
    assert cluster_by_error([]) == []


def test_cluster_no_error_field():
    runs = [make_run(error=None)]
    clusters = cluster_by_error(runs)
    assert clusters[0].key == "(no error)"


def test_cluster_sorted_by_count_desc():
    runs = [
        make_run(pipeline="a", error="alpha error"),
        make_run(pipeline="b", error="beta error"),
        make_run(pipeline="c", error="beta error"),
    ]
    clusters = cluster_by_error(runs)
    assert clusters[0].count >= clusters[-1].count


def test_cluster_pipelines_list():
    runs = [
        make_run(pipeline="pipe_a", error="disk full"),
        make_run(pipeline="pipe_b", error="disk full"),
    ]
    clusters = cluster_by_error(runs)
    assert set(clusters[0].pipelines) == {"pipe_a", "pipe_b"}
