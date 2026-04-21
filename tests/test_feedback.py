"""Tests for pipewatch.feedback."""

from __future__ import annotations

import pytest

from pipewatch.feedback import (
    FeedbackEntry,
    add_feedback,
    is_suppressed,
    load_feedback,
    save_feedback,
    suppressed_keys,
)


@pytest.fixture()
def store_path(tmp_path):
    return str(tmp_path / "runs" / "runs.jsonl")


def test_load_feedback_missing_returns_empty(store_path):
    assert load_feedback(store_path) == []


def test_add_feedback_creates_entry(store_path):
    entry = add_feedback(store_path, "pipe_a:consecutive_failures", "acknowledged", note="looking into it")
    assert entry.alert_key == "pipe_a:consecutive_failures"
    assert entry.action == "acknowledged"
    assert entry.note == "looking into it"
    assert entry.timestamp  # non-empty


def test_add_feedback_persists(store_path):
    add_feedback(store_path, "pipe_a:low_success_rate", "resolved")
    loaded = load_feedback(store_path)
    assert len(loaded) == 1
    assert loaded[0].action == "resolved"


def test_multiple_entries_appended(store_path):
    add_feedback(store_path, "pipe_a:x", "acknowledged")
    add_feedback(store_path, "pipe_b:y", "suppressed")
    loaded = load_feedback(store_path)
    assert len(loaded) == 2


def test_suppressed_keys_returns_suppressed(store_path):
    add_feedback(store_path, "pipe_a:x", "suppressed")
    add_feedback(store_path, "pipe_b:y", "acknowledged")
    keys = suppressed_keys(store_path)
    assert "pipe_a:x" in keys
    assert "pipe_b:y" not in keys


def test_suppression_lifted_by_later_entry(store_path):
    add_feedback(store_path, "pipe_a:x", "suppressed")
    add_feedback(store_path, "pipe_a:x", "resolved")  # lifts suppression
    assert not is_suppressed(store_path, "pipe_a:x")


def test_is_suppressed_true(store_path):
    add_feedback(store_path, "pipe_z:failures", "suppressed")
    assert is_suppressed(store_path, "pipe_z:failures")


def test_is_suppressed_false_for_unknown(store_path):
    assert not is_suppressed(store_path, "nonexistent:key")


def test_entry_roundtrip():
    e = FeedbackEntry(alert_key="p:k", action="resolved", note="fixed", timestamp="2024-01-01T00:00:00+00:00")
    assert FeedbackEntry.from_dict(e.to_dict()) == e


def test_entry_str_with_note():
    e = FeedbackEntry(alert_key="p:k", action="suppressed", note="noise", timestamp="2024-01-01T00:00:00+00:00")
    assert "SUPPRESSED" in str(e)
    assert "noise" in str(e)


def test_entry_str_without_note():
    e = FeedbackEntry(alert_key="p:k", action="acknowledged", note=None, timestamp="2024-01-01T00:00:00+00:00")
    result = str(e)
    assert "ACKNOWLEDGED" in result
    assert "—" not in result
