"""Tests for pipewatch.signal — signal detection."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.signal import SignalResult, _transitions, detect_signals


def make_run(
    pipeline: str,
    status: str,
    offset_seconds: int = 0,
) -> PipelineRun:
    ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    from datetime import timedelta

    started = ts + timedelta(seconds=offset_seconds)
    ended = started + timedelta(seconds=30)
    return PipelineRun(
        run_id=f"{pipeline}-{offset_seconds}",
        pipeline=pipeline,
        status=PipelineStatus(status),
        started_at=started.isoformat(),
        ended_at=ended.isoformat(),
    )


def _runs(pipeline: str, pattern: List[str]) -> List[PipelineRun]:
    """Build runs from a list of 'success'/'failed' strings."""
    return [make_run(pipeline, s, i * 60) for i, s in enumerate(pattern)]


# ---------------------------------------------------------------------------
# _transitions helper
# ---------------------------------------------------------------------------

def test_transitions_no_flips():
    assert _transitions([True, True, True]) == 0


def test_transitions_all_flips():
    assert _transitions([True, False, True, False]) == 3


def test_transitions_single_element():
    assert _transitions([True]) == 0


# ---------------------------------------------------------------------------
# detect_signals
# ---------------------------------------------------------------------------

def test_detect_signals_stable():
    runs = _runs("pipe-a", ["success"] * 8)
    results = detect_signals(runs, min_runs=4)
    assert len(results) == 1
    assert results[0].signal == "stable"
    assert results[0].confidence == pytest.approx(1.0)


def test_detect_signals_flapping():
    pattern = ["success", "failed"] * 5  # 10 runs, 9 flips
    runs = _runs("flappy", pattern)
    results = detect_signals(runs, min_runs=4)
    assert len(results) == 1
    assert results[0].signal == "flapping"
    assert results[0].confidence > 0.5


def test_detect_signals_degrading():
    # Early runs succeed, recent runs fail
    pattern = ["success"] * 5 + ["failed"] * 5
    runs = _runs("degrades", pattern)
    results = detect_signals(runs, min_runs=4)
    assert len(results) == 1
    assert results[0].signal == "degrading"


def test_detect_signals_recovering():
    # Early runs fail, recent runs succeed
    pattern = ["failed"] * 5 + ["success"] * 5
    runs = _runs("recovers", pattern)
    results = detect_signals(runs, min_runs=4)
    assert len(results) == 1
    assert results[0].signal == "recovering"


def test_detect_signals_insufficient_data():
    runs = _runs("tiny", ["success", "failed", "success"])
    results = detect_signals(runs, min_runs=4)
    assert results == []


def test_detect_signals_filter_pipeline():
    runs = _runs("pipe-a", ["success"] * 6) + _runs("pipe-b", ["failed"] * 6)
    results = detect_signals(runs, pipeline="pipe-a", min_runs=4)
    assert all(r.pipeline == "pipe-a" for r in results)
    assert len(results) == 1


def test_detect_signals_multiple_pipelines():
    runs = _runs("alpha", ["success"] * 6) + _runs("beta", ["success", "failed"] * 4)
    results = detect_signals(runs, min_runs=4)
    names = {r.pipeline for r in results}
    assert names == {"alpha", "beta"}


def test_signal_result_str_contains_pipeline():
    r = SignalResult("my-pipeline", "stable", 0.9, "9/10 recent runs succeeded")
    assert "my-pipeline" in str(r)
    assert "stable" in str(r)
    assert "90%" in str(r)
