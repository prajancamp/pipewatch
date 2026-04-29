"""Tests for pipewatch.succession"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Optional

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.succession import detect_succession, SuccessionResult


_BASE = datetime(2024, 1, 1, 12, 0, 0)


def make_run(
    pipeline: str,
    status: PipelineStatus,
    offset_seconds: float = 0.0,
    duration: Optional[float] = 10.0,
) -> PipelineRun:
    started = _BASE + timedelta(seconds=offset_seconds)
    ended = started + timedelta(seconds=duration) if duration is not None else None
    return PipelineRun(
        run_id=f"{pipeline}-{offset_seconds}",
        pipeline=pipeline,
        status=status,
        started_at=started,
        ended_at=ended,
    )


def test_detect_succession_empty():
    assert detect_succession([]) == []


def test_no_succession_all_success():
    runs = [
        make_run("a", PipelineStatus.SUCCESS, 0),
        make_run("b", PipelineStatus.SUCCESS, 60),
    ]
    assert detect_succession(runs) == []


def test_no_succession_below_min_occurrences():
    # Only one co-occurrence — below the default min_occurrences=2
    runs = [
        make_run("a", PipelineStatus.FAILED, 0),
        make_run("b", PipelineStatus.FAILED, 30),
    ]
    results = detect_succession(runs, window_seconds=60, min_occurrences=2)
    assert results == []


def test_detects_succession_pattern():
    # 'a' fails at t=0 and t=1000; 'b' fails at t=30 and t=1030 — both within 60 s
    runs = [
        make_run("a", PipelineStatus.FAILED, 0),
        make_run("b", PipelineStatus.FAILED, 30),
        make_run("a", PipelineStatus.FAILED, 1000),
        make_run("b", PipelineStatus.FAILED, 1030),
    ]
    results = detect_succession(runs, window_seconds=60, min_rate=0.5, min_occurrences=2)
    assert len(results) == 1
    r = results[0]
    assert r.trigger_pipeline == "a"
    assert r.successor_pipeline == "b"
    assert r.trigger_failures == 2
    assert r.successor_failures_after == 2
    assert r.rate == pytest.approx(1.0)


def test_succession_rate_below_threshold_excluded():
    # 'a' fails 4 times; 'b' follows only once — rate = 0.25 < min_rate=0.5
    runs = [
        make_run("a", PipelineStatus.FAILED, 0),
        make_run("b", PipelineStatus.FAILED, 30),
        make_run("a", PipelineStatus.FAILED, 1000),
        make_run("a", PipelineStatus.FAILED, 2000),
        make_run("a", PipelineStatus.FAILED, 3000),
    ]
    results = detect_succession(runs, window_seconds=60, min_rate=0.5, min_occurrences=1)
    assert results == []


def test_filter_by_trigger_pipeline():
    runs = [
        make_run("a", PipelineStatus.FAILED, 0),
        make_run("b", PipelineStatus.FAILED, 20),
        make_run("a", PipelineStatus.FAILED, 1000),
        make_run("b", PipelineStatus.FAILED, 1020),
        make_run("c", PipelineStatus.FAILED, 5),
        make_run("b", PipelineStatus.FAILED, 25),
        make_run("c", PipelineStatus.FAILED, 1005),
        make_run("b", PipelineStatus.FAILED, 1025),
    ]
    results = detect_succession(runs, window_seconds=60, min_rate=0.5,
                                min_occurrences=2, pipeline="a")
    for r in results:
        assert r.trigger_pipeline == "a"


def test_str_representation():
    r = SuccessionResult(
        trigger_pipeline="ingest",
        successor_pipeline="transform",
        trigger_failures=4,
        successor_failures_after=3,
        rate=0.75,
    )
    s = str(r)
    assert "transform" in s
    assert "ingest" in s
    assert "75%" in s


def test_results_sorted_by_rate_descending():
    # Two successor candidates with different rates
    runs = [
        # trigger 'a' fails 2×
        make_run("a", PipelineStatus.FAILED, 0),
        make_run("a", PipelineStatus.FAILED, 1000),
        # 'b' follows both (rate 1.0)
        make_run("b", PipelineStatus.FAILED, 30),
        make_run("b", PipelineStatus.FAILED, 1030),
        # 'c' follows only one (rate 0.5)
        make_run("c", PipelineStatus.FAILED, 50),
    ]
    results = detect_succession(runs, window_seconds=60, min_rate=0.4, min_occurrences=1)
    rates = [r.rate for r in results]
    assert rates == sorted(rates, reverse=True)
