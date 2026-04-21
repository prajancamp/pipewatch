"""Tests for pipewatch.cooldown."""
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from pipewatch.cooldown import (
    check_cooldown_status,
    clear_cooldown,
    is_in_cooldown,
    load_cooldown_state,
    record_cooldown,
    save_cooldown_state,
)


@pytest.fixture()
def store_path(tmp_path: Path) -> str:
    return str(tmp_path / "runs.jsonl")


NOW = datetime(2024, 6, 1, 12, 0, 0)


def test_load_missing_returns_empty(store_path):
    assert load_cooldown_state(store_path) == {}


def test_save_and_load_roundtrip(store_path):
    state = {"pipeline_a": NOW.isoformat(), "pipeline_b": NOW.isoformat()}
    save_cooldown_state(store_path, state)
    loaded = load_cooldown_state(store_path)
    assert loaded == state


def test_is_in_cooldown_unknown_pipeline(store_path):
    assert is_in_cooldown(store_path, "unknown", cooldown_minutes=30, now=NOW) is False


def test_is_in_cooldown_within_window(store_path):
    recent = NOW - timedelta(minutes=10)
    record_cooldown(store_path, "pipe_a", now=recent)
    assert is_in_cooldown(store_path, "pipe_a", cooldown_minutes=30, now=NOW) is True


def test_is_in_cooldown_after_window(store_path):
    old = NOW - timedelta(minutes=60)
    record_cooldown(store_path, "pipe_a", now=old)
    assert is_in_cooldown(store_path, "pipe_a", cooldown_minutes=30, now=NOW) is False


def test_is_in_cooldown_exactly_at_boundary(store_path):
    # exactly at boundary: timedelta == cooldown => NOT in cooldown (strict <)
    boundary = NOW - timedelta(minutes=30)
    record_cooldown(store_path, "pipe_a", now=boundary)
    assert is_in_cooldown(store_path, "pipe_a", cooldown_minutes=30, now=NOW) is False


def test_record_cooldown_persists(store_path):
    record_cooldown(store_path, "pipe_x", now=NOW)
    state = load_cooldown_state(store_path)
    assert "pipe_x" in state
    assert state["pipe_x"] == NOW.isoformat()


def test_record_cooldown_overwrites(store_path):
    old = NOW - timedelta(hours=2)
    record_cooldown(store_path, "pipe_x", now=old)
    record_cooldown(store_path, "pipe_x", now=NOW)
    state = load_cooldown_state(store_path)
    assert state["pipe_x"] == NOW.isoformat()


def test_clear_cooldown_removes_entry(store_path):
    record_cooldown(store_path, "pipe_x", now=NOW)
    clear_cooldown(store_path, "pipe_x")
    assert is_in_cooldown(store_path, "pipe_x", now=NOW) is False


def test_clear_cooldown_missing_is_noop(store_path):
    clear_cooldown(store_path, "nonexistent")  # should not raise


def test_check_cooldown_status_ready(store_path):
    status = check_cooldown_status(store_path, "pipe_a", cooldown_minutes=30, now=NOW)
    assert status.in_cooldown is False
    assert status.last_fired is None
    assert "ready" in str(status)


def test_check_cooldown_status_in_cooldown(store_path):
    record_cooldown(store_path, "pipe_a", now=NOW - timedelta(minutes=5))
    status = check_cooldown_status(store_path, "pipe_a", cooldown_minutes=30, now=NOW)
    assert status.in_cooldown is True
    assert status.last_fired is not None
    assert "IN COOLDOWN" in str(status)
