"""Tests for pipewatch.mute."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.mute import (
    MuteRule,
    add_mute_rule,
    is_muted,
    load_mute_rules,
    remove_expired_rules,
    save_mute_rules,
)


@pytest.fixture
def store_path(tmp_path: Path) -> str:
    return str(tmp_path / "runs.jsonl")


def _future(hours: float = 2) -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()


def _past(hours: float = 2) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


def test_load_missing_returns_empty(store_path):
    assert load_mute_rules(store_path) == []


def test_add_and_load(store_path):
    rule = MuteRule(pipeline="etl_*", reason="maintenance", expires_at=_future())
    add_mute_rule(store_path, rule)
    rules = load_mute_rules(store_path)
    assert len(rules) == 1
    assert rules[0].pipeline == "etl_*"
    assert rules[0].reason == "maintenance"


def test_is_expired_future(store_path):
    rule = MuteRule(pipeline="p", reason="r", expires_at=_future())
    assert not rule.is_expired()


def test_is_expired_past(store_path):
    rule = MuteRule(pipeline="p", reason="r", expires_at=_past())
    assert rule.is_expired()


def test_is_expired_permanent():
    rule = MuteRule(pipeline="p", reason="r", expires_at=None)
    assert not rule.is_expired()


def test_matches_exact():
    rule = MuteRule(pipeline="my_pipeline", reason="r", expires_at=None)
    assert rule.matches("my_pipeline")
    assert not rule.matches("other_pipeline")


def test_matches_glob():
    rule = MuteRule(pipeline="etl_*", reason="r", expires_at=None)
    assert rule.matches("etl_daily")
    assert rule.matches("etl_hourly")
    assert not rule.matches("reporting_daily")


def test_is_muted_active_rule():
    rules = [MuteRule(pipeline="etl_*", reason="r", expires_at=_future())]
    assert is_muted("etl_daily", rules)
    assert not is_muted("reporting", rules)


def test_is_muted_expired_rule():
    rules = [MuteRule(pipeline="etl_*", reason="r", expires_at=_past())]
    assert not is_muted("etl_daily", rules)


def test_remove_expired_rules(store_path):
    rules = [
        MuteRule(pipeline="a", reason="r", expires_at=_past()),
        MuteRule(pipeline="b", reason="r", expires_at=_future()),
        MuteRule(pipeline="c", reason="r", expires_at=None),
    ]
    save_mute_rules(store_path, rules)
    removed = remove_expired_rules(store_path)
    assert removed == 1
    remaining = load_mute_rules(store_path)
    assert len(remaining) == 2
    assert all(r.pipeline != "a" for r in remaining)


def test_roundtrip_serialisation(store_path):
    rule = MuteRule(pipeline="pipe", reason="test", expires_at=_future())
    add_mute_rule(store_path, rule)
    loaded = load_mute_rules(store_path)[0]
    assert loaded.pipeline == rule.pipeline
    assert loaded.reason == rule.reason
    assert loaded.expires_at == rule.expires_at


def test_str_representation():
    rule = MuteRule(pipeline="p", reason="r", expires_at=None)
    assert "permanent" in str(rule)
    assert "p" in str(rule)
