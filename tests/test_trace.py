"""Tests for pipewatch.trace module."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.trace import TraceChain, build_traces, failing_traces, trace_summary


def make_run(
    pipeline: str = "pipe",
    status: PipelineStatus = PipelineStatus.SUCCESS,
    trace_id: Optional[str] = None,
    duration: Optional[float] = 10.0,
    error: Optional[str] = None,
    started_at: str = "2024-01-01T10:00:00",
) -> PipelineRun:
    meta = {"trace_id": trace_id} if trace_id else {}
    return PipelineRun(
        run_id=str(uuid.uuid4()),
        pipeline=pipeline,
        status=status,
        started_at=started_at,
        finished_at="2024-01-01T10:00:10",
        duration=duration,
        error=error,
        tags=[],
        meta=meta,
    )


def test_build_traces_groups_by_trace_id():
    runs = [
        make_run(pipeline="a", trace_id="t1"),
        make_run(pipeline="b", trace_id="t1"),
        make_run(pipeline="c", trace_id="t2"),
    ]
    chains = build_traces(runs)
    assert set(chains.keys()) == {"t1", "t2"}
    assert len(chains["t1"].runs) == 2
    assert len(chains["t2"].runs) == 1


def test_build_traces_ignores_runs_without_trace_id():
    runs = [
        make_run(pipeline="a", trace_id="t1"),
        make_run(pipeline="b"),  # no trace_id
    ]
    chains = build_traces(runs)
    assert len(chains) == 1
    assert "t1" in chains


def test_build_traces_empty_input():
    assert build_traces([]) == {}


def test_trace_chain_has_failure_true():
    chain = TraceChain(trace_id="t1", runs=[
        make_run(status=PipelineStatus.SUCCESS),
        make_run(status=PipelineStatus.FAILED),
    ])
    assert chain.has_failure is True


def test_trace_chain_has_failure_false():
    chain = TraceChain(trace_id="t1", runs=[
        make_run(status=PipelineStatus.SUCCESS),
        make_run(status=PipelineStatus.SUCCESS),
    ])
    assert chain.has_failure is False


def test_trace_chain_total_duration():
    chain = TraceChain(trace_id="t1", runs=[
        make_run(duration=5.0),
        make_run(duration=15.0),
    ])
    assert chain.total_duration == pytest.approx(20.0)


def test_trace_chain_total_duration_none_when_all_missing():
    chain = TraceChain(trace_id="t1", runs=[
        make_run(duration=None),
    ])
    assert chain.total_duration is None


def test_trace_chain_pipelines():
    chain = TraceChain(trace_id="t1", runs=[
        make_run(pipeline="alpha"),
        make_run(pipeline="beta"),
    ])
    assert chain.pipelines == ["alpha", "beta"]


def test_failing_traces_filters_correctly():
    runs_t1 = [make_run(trace_id="t1", status=PipelineStatus.SUCCESS)]
    runs_t2 = [make_run(trace_id="t2", status=PipelineStatus.FAILED)]
    chains = {
        "t1": TraceChain(trace_id="t1", runs=runs_t1),
        "t2": TraceChain(trace_id="t2", runs=runs_t2),
    }
    result = failing_traces(chains)
    assert len(result) == 1
    assert result[0].trace_id == "t2"


def test_trace_summary_counts():
    chains = {
        "t1": TraceChain(trace_id="t1", runs=[make_run(status=PipelineStatus.SUCCESS)]),
        "t2": TraceChain(trace_id="t2", runs=[make_run(status=PipelineStatus.FAILED)]),
        "t3": TraceChain(trace_id="t3", runs=[make_run(status=PipelineStatus.SUCCESS)]),
    }
    s = trace_summary(chains)
    assert s["total"] == 3
    assert s["failed"] == 1
    assert s["healthy"] == 2


def test_trace_chain_str_ok():
    chain = TraceChain(trace_id="abc", runs=[make_run(status=PipelineStatus.SUCCESS, duration=5.0)])
    out = str(chain)
    assert "abc" in out
    assert "OK" in out


def test_trace_chain_str_fail():
    chain = TraceChain(trace_id="xyz", runs=[make_run(status=PipelineStatus.FAILED, duration=3.0)])
    out = str(chain)
    assert "FAIL" in out
