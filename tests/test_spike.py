"""Tests for pipewatch.spike — failure spike detection."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.spike import SpikeResult, detect_spikes


def make_run(
    pipeline: str,
    status: PipelineStatus,
    minutes_ago: float,
    error: Optional[str] = None,
) -> PipelineRun:
    now = datetime.now(tz=timezone.utc)
    started = now - timedelta(minutes=minutes_ago)
    ended = started + timedelta(seconds=10)
    return PipelineRun(
        run_id=f"{pipeline}-{minutes_ago}",
        pipeline=pipeline,
        status=status,
        started_at=started,
        ended_at=ended,
        error=error,
    )


def test_detect_spikes_empty():
    assert detect_spikes([]) == []


def test_no_spike_when_no_recent_runs():
    # All runs are in the baseline window, none in the recent window
    runs = [make_run("pipe", PipelineStatus.FAILED, 60) for _ in range(5)]
    results = detect_spikes(runs, window_minutes=10, lookback_minutes=120)
    assert results == []


def test_no_spike_all_success():
    runs = [make_run("pipe", PipelineStatus.SUCCESS, i) for i in range(1, 20)]
    results = detect_spikes(runs, window_minutes=30, lookback_minutes=360)
    assert len(results) == 1
    assert not results[0].flagged


def test_spike_detected_when_rate_exceeds_multiplier():
    # Baseline: 1/10 failures (10%)
    baseline = [
        make_run("pipe", PipelineStatus.FAILED if i == 0 else PipelineStatus.SUCCESS, 60 + i)
        for i in range(10)
    ]
    # Recent window: 3/4 failures (75%)
    recent = [
        make_run("pipe", PipelineStatus.FAILED if i < 3 else PipelineStatus.SUCCESS, i + 1)
        for i in range(4)
    ]
    results = detect_spikes(
        baseline + recent,
        window_minutes=30,
        lookback_minutes=360,
        threshold_multiplier=2.0,
        min_spike_count=2,
    )
    assert len(results) == 1
    r = results[0]
    assert r.flagged
    assert r.spike_count == 3
    assert r.total_recent == 4


def test_spike_not_flagged_below_min_count():
    baseline = [make_run("pipe", PipelineStatus.SUCCESS, 60 + i) for i in range(10)]
    recent = [
        make_run("pipe", PipelineStatus.FAILED, 1),
        make_run("pipe", PipelineStatus.SUCCESS, 2),
        make_run("pipe", PipelineStatus.SUCCESS, 3),
    ]
    results = detect_spikes(
        baseline + recent,
        window_minutes=30,
        lookback_minutes=360,
        min_spike_count=3,  # require 3 failures to flag
    )
    assert len(results) == 1
    assert not results[0].flagged


def test_filter_by_pipeline():
    runs_a = [make_run("alpha", PipelineStatus.FAILED, i + 1) for i in range(4)]
    runs_b = [make_run("beta", PipelineStatus.FAILED, i + 1) for i in range(4)]
    results = detect_spikes(runs_a + runs_b, pipeline="alpha")
    assert all(r.pipeline == "alpha" for r in results)


def test_spike_str_contains_pipeline():
    r = SpikeResult(
        pipeline="my_pipe",
        window_minutes=30,
        baseline_failure_rate=0.1,
        spike_failure_rate=0.8,
        spike_count=4,
        total_recent=5,
        flagged=True,
    )
    assert "my_pipe" in str(r)
    assert "SPIKE" in str(r)


def test_ok_str_shows_ok():
    r = SpikeResult(
        pipeline="stable",
        window_minutes=30,
        baseline_failure_rate=0.05,
        spike_failure_rate=0.1,
        spike_count=1,
        total_recent=10,
        flagged=False,
    )
    assert "ok" in str(r)
