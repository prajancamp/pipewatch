"""Tests for pipewatch.triage."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.triage import TriageResult, triage_runs


def make_run(
    pipeline: str = "pipe",
    status: PipelineStatus = PipelineStatus.SUCCESS,
    started: datetime = datetime(2024, 1, 1, tzinfo=timezone.utc),
    duration: Optional[float] = 10.0,
    error: Optional[str] = None,
) -> PipelineRun:
    return PipelineRun(
        run_id=f"{pipeline}-{started.isoformat()}",
        pipeline=pipeline,
        status=status,
        started_at=started,
        duration=duration,
        error=error,
    )


def _make_failures(pipeline: str, n: int) -> List[PipelineRun]:
    from datetime import timedelta

    base = datetime(2024, 6, 1, tzinfo=timezone.utc)
    return [
        make_run(pipeline, PipelineStatus.FAILED, base + timedelta(hours=i))
        for i in range(n)
    ]


def test_triage_empty_returns_empty():
    assert triage_runs([]) == []


def test_triage_all_healthy_below_default_threshold():
    runs = [make_run("p", PipelineStatus.SUCCESS) for _ in range(5)]
    results = triage_runs(runs, min_priority=1)
    assert results == []


def test_triage_critical_on_many_consecutive_failures():
    runs = _make_failures("bad_pipe", 6)
    results = triage_runs(runs, min_priority=0)
    assert len(results) == 1
    r = results[0]
    assert r.pipeline == "bad_pipe"
    assert r.priority == 3
    assert r.label == "CRITICAL"


def test_triage_high_on_low_success_rate():
    from datetime import timedelta

    base = datetime(2024, 6, 1, tzinfo=timezone.utc)
    runs = [
        make_run("flaky", PipelineStatus.FAILED, base + timedelta(hours=i))
        for i in range(4)
    ] + [
        make_run("flaky", PipelineStatus.SUCCESS, base + timedelta(hours=4))
    ]
    results = triage_runs(runs, min_priority=0)
    assert any(r.pipeline == "flaky" and r.priority >= 2 for r in results)


def test_triage_filter_by_pipeline():
    runs = _make_failures("alpha", 5) + _make_failures("beta", 5)
    results = triage_runs(runs, min_priority=0, pipeline="alpha")
    assert all(r.pipeline == "alpha" for r in results)
    assert len(results) == 1


def test_triage_sorted_by_priority_descending():
    runs = _make_failures("critical_pipe", 6) + _make_failures("medium_pipe", 3)
    results = triage_runs(runs, min_priority=0)
    priorities = [r.priority for r in results]
    assert priorities == sorted(priorities, reverse=True)


def test_triage_result_str_contains_label():
    r = TriageResult(pipeline="p", priority=3, score=6.0, reasons=["5 consecutive failures"])
    assert "CRITICAL" in str(r)
    assert "p" in str(r)
