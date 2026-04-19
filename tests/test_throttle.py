"""Tests for pipewatch.throttle."""

import pytest
from pathlib import Path
from pipewatch.throttle import (
    is_throttled,
    record_fired,
    load_throttle_state,
    save_throttle_state,
    filter_throttled_alerts,
)

NOW = 1_700_000_000.0
COOLDOWN = 3600.0


def test_is_throttled_unknown_key():
    assert is_throttled("k", {}, COOLDOWN, NOW) is False


def test_is_throttled_within_cooldown():
    state = {"k": NOW - 100}
    assert is_throttled("k", state, COOLDOWN, NOW) is True


def test_is_throttled_after_cooldown():
    state = {"k": NOW - COOLDOWN - 1}
    assert is_throttled("k", state, COOLDOWN, NOW) is False


def test_is_throttled_exactly_at_boundary():
    state = {"k": NOW - COOLDOWN}
    assert is_throttled("k", state, COOLDOWN, NOW) is False


def test_record_fired_adds_key():
    state = record_fired("k", {}, NOW)
    assert state["k"] == NOW


def test_record_fired_does_not_mutate_original():
    original = {"x": 1.0}
    record_fired("k", original, NOW)
    assert "k" not in original


def test_save_and_load_roundtrip(tmp_path):
    state = {"alert:pipe_a": NOW, "alert:pipe_b": NOW - 500}
    save_throttle_state(tmp_path, state)
    loaded = load_throttle_state(tmp_path)
    assert loaded == state


def test_load_missing_returns_empty(tmp_path):
    assert load_throttle_state(tmp_path) == {}


def test_filter_throttled_alerts_all_new(tmp_path):
    keys = ["a", "b", "c"]
    active, state = filter_throttled_alerts(keys, tmp_path, COOLDOWN, NOW)
    assert active == keys
    assert set(state.keys()) == {"a", "b", "c"}


def test_filter_throttled_alerts_some_suppressed(tmp_path):
    save_throttle_state(tmp_path, {"a": NOW - 100})  # still within cooldown
    active, state = filter_throttled_alerts(["a", "b"], tmp_path, COOLDOWN, NOW)
    assert active == ["b"]
    assert "b" in state


def test_filter_throttled_alerts_all_suppressed(tmp_path):
    save_throttle_state(tmp_path, {"a": NOW - 10, "b": NOW - 20})
    active, _ = filter_throttled_alerts(["a", "b"], tmp_path, COOLDOWN, NOW)
    assert active == []
