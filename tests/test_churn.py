"""Tests for pipewatch.churn."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from pipewatch.churn import ChurnResult, _count_transitions, detect_churn
from pipewatch.models import PipelineRun, PipelineStatus

_BASE = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def make_run(
    pipeline: str = "pipe-a",
    status: PipelineStatus = PipelineStatus.SUCCESS,
    offset_minutes: int = 0,
) -> PipelineRun:
    started = _BASE + timedelta(minutes=offset_minutes)
    return PipelineRun(
        run_id=f"{pipeline}-{offset_minutes}",
        pipeline=pipeline,
        status=status,
        started_at=started,
        ended_at=started + timedelta(minutes=1),
    )


# ---------------------------------------------------------------------------
# _count_transitions
# ---------------------------------------------------------------------------

def test_transitions_no_flips():
    runs = [make_run(status=PipelineStatus.SUCCESS, offset_minutes=i) for i in range(5)]
    assert _count_transitions(runs) == 0


def test_transitions_all_flips():
    statuses = [PipelineStatus.SUCCESS, PipelineStatus.FAILED] * 3
    runs = [make_run(status=s, offset_minutes=i) for i, s in enumerate(statuses)]
    assert _count_transitions(runs) == 5


def test_transitions_single_element():
    assert _count_transitions([make_run()]) == 0


# ---------------------------------------------------------------------------
# detect_churn
# ---------------------------------------------------------------------------

def _now_patch():
    return _BASE + timedelta(hours=1)   # window covers all _BASE runs


def test_detect_churn_below_min_runs():
    runs = [make_run(offset_minutes=i) for i in range(3)]
    with patch("pipewatch.churn._now", _now_patch):
        results = detect_churn(runs, window_hours=24, min_runs=4)
    assert results == []


def test_detect_churn_stable_pipeline():
    runs = [make_run(status=PipelineStatus.SUCCESS, offset_minutes=i) for i in range(6)]
    with patch("pipewatch.churn._now", _now_patch):
        results = detect_churn(runs, window_hours=24, min_runs=4, churn_threshold=0.5)
    assert len(results) == 1
    assert results[0].is_churning is False
    assert results[0].transitions == 0


def test_detect_churn_churning_pipeline():
    statuses = [PipelineStatus.SUCCESS, PipelineStatus.FAILED] * 4
    runs = [make_run(status=s, offset_minutes=i) for i, s in enumerate(statuses)]
    with patch("pipewatch.churn._now", _now_patch):
        results = detect_churn(runs, window_hours=24, min_runs=4, churn_threshold=0.5)
    assert len(results) == 1
    r = results[0]
    assert r.is_churning is True
    assert r.transitions == 7
    assert r.churn_rate == pytest.approx(7 / 7)


def test_detect_churn_excludes_old_runs():
    old_runs = [
        make_run(status=PipelineStatus.FAILED, offset_minutes=-(60 * 48 + i))
        for i in range(6)
    ]
    with patch("pipewatch.churn._now", _now_patch):
        results = detect_churn(old_runs, window_hours=24, min_runs=4)
    assert results == []


def test_detect_churn_filter_by_pipeline():
    runs_a = [make_run(pipeline="a", status=PipelineStatus.SUCCESS, offset_minutes=i) for i in range(6)]
    runs_b = [
        make_run(
            pipeline="b",
            status=PipelineStatus.SUCCESS if i % 2 == 0 else PipelineStatus.FAILED,
            offset_minutes=i,
        )
        for i in range(6)
    ]
    with patch("pipewatch.churn._now", _now_patch):
        results = detect_churn(runs_a + runs_b, window_hours=24, min_runs=4, pipeline="b")
    assert all(r.pipeline == "b" for r in results)


def test_churn_result_str_churning():
    r = ChurnResult(pipeline="etl", window_hours=24, total_runs=8, transitions=6, churn_rate=0.857, is_churning=True)
    assert "CHURNING" in str(r)
    assert "etl" in str(r)


def test_churn_result_str_ok():
    r = ChurnResult(pipeline="etl", window_hours=24, total_runs=8, transitions=1, churn_rate=0.14, is_churning=False)
    assert "OK" in str(r)
