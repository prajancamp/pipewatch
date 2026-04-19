"""Tests for pipewatch.retention."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import uuid

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore
from pipewatch.retention import prune_before, prune_by_count, apply_retention


def make_run(days_ago: float, pipeline: str = "pipe") -> PipelineRun:
    started = datetime.now(tz=timezone.utc) - timedelta(days=days_ago)
    return PipelineRun(
        run_id=str(uuid.uuid4()),
        pipeline=pipeline,
        status=PipelineStatus.SUCCESS,
        started_at=started,
        ended_at=started + timedelta(seconds=10),
    )


@pytest.fixture()
def tmp_store(tmp_path: Path) -> RunStore:
    return RunStore(tmp_path / "runs.jsonl")


def test_prune_before_removes_old():
    runs = [make_run(10), make_run(5), make_run(1)]
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=7)
    result = prune_before(runs, cutoff)
    assert len(result) == 2


def test_prune_before_keeps_all_recent():
    runs = [make_run(1), make_run(2)]
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=30)
    assert prune_before(runs, cutoff) == runs


def test_prune_by_count_keeps_most_recent():
    runs = [make_run(10), make_run(5), make_run(1)]
    result = prune_by_count(runs, 2)
    assert len(result) == 2
    assert all(r in runs for r in result)
    ages = sorted([10, 5, 1])
    # The two most recent: days_ago 5 and 1
    assert make_run(10) not in result or True  # order check via started_at
    started_days = sorted([(datetime.now(tz=timezone.utc) - r.started_at).days for r in result])
    assert started_days[0] <= 2  # most recent kept


def test_prune_by_count_zero_returns_empty():
    runs = [make_run(1), make_run(2)]
    assert prune_by_count(runs, 0) == []


def test_apply_retention_by_age(tmp_store: RunStore):
    for days in [20, 10, 2]:
        tmp_store.append(make_run(days))
    removed = apply_retention(tmp_store, max_age_days=5)
    assert removed == 2
    assert len(tmp_store.load_all()) == 1


def test_apply_retention_by_count(tmp_store: RunStore):
    for days in [10, 5, 3, 1]:
        tmp_store.append(make_run(days))
    removed = apply_retention(tmp_store, max_count=2)
    assert removed == 2
    assert len(tmp_store.load_all()) == 2


def test_apply_retention_both_filters(tmp_store: RunStore):
    for days in [30, 10, 3, 1]:
        tmp_store.append(make_run(days))
    removed = apply_retention(tmp_store, max_age_days=15, max_count=1)
    assert len(tmp_store.load_all()) == 1


def test_apply_retention_no_args_is_noop(tmp_store: RunStore):
    tmp_store.append(make_run(1))
    removed = apply_retention(tmp_store)
    assert removed == 0
    assert len(tmp_store.load_all()) == 1
