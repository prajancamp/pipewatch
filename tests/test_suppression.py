"""Tests for pipewatch.suppression."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.alert import Alert, AlertRule
from pipewatch.suppression import (
    SuppressionRule,
    add_rule,
    load_rules,
    save_rules,
    suppress_alerts,
)


def _make_alert(pipeline: str, rule_name: str) -> Alert:
    rule = AlertRule(name=rule_name, min_success_rate=0.8)
    return Alert(pipeline=pipeline, rule=rule, message="test alert")


@pytest.fixture
def store_path(tmp_path: Path) -> str:
    store_file = tmp_path / "runs" / "runs.jsonl"
    store_file.parent.mkdir(parents=True, exist_ok=True)
    store_file.touch()
    return str(store_file)


def test_load_rules_missing_returns_empty(store_path):
    rules = load_rules(store_path)
    assert rules == []


def test_save_and_load_roundtrip(store_path):
    rules = [
        SuppressionRule(pipeline="etl", alert_type="low_success", reason="known issue"),
        SuppressionRule(pipeline=None, alert_type="consecutive_failures", reason="flaky"),
    ]
    save_rules(store_path, rules)
    loaded = load_rules(store_path)
    assert len(loaded) == 2
    assert loaded[0].pipeline == "etl"
    assert loaded[0].alert_type == "low_success"
    assert loaded[0].reason == "known issue"
    assert loaded[1].pipeline is None


def test_add_rule_appends(store_path):
    add_rule(store_path, SuppressionRule(pipeline="pipe_a", reason="r1"))
    add_rule(store_path, SuppressionRule(pipeline="pipe_b", reason="r2"))
    rules = load_rules(store_path)
    assert len(rules) == 2
    assert {r.pipeline for r in rules} == {"pipe_a", "pipe_b"}


def test_rule_matches_by_pipeline():
    rule = SuppressionRule(pipeline="etl", alert_type=None)
    alert_match = _make_alert("etl", "low_success")
    alert_no_match = _make_alert("other", "low_success")
    assert rule.matches(alert_match) is True
    assert rule.matches(alert_no_match) is False


def test_rule_matches_by_alert_type():
    rule = SuppressionRule(pipeline=None, alert_type="low_success")
    alert_match = _make_alert("any_pipe", "low_success")
    alert_no_match = _make_alert("any_pipe", "consecutive_failures")
    assert rule.matches(alert_match) is True
    assert rule.matches(alert_no_match) is False


def test_rule_matches_both_fields():
    rule = SuppressionRule(pipeline="etl", alert_type="low_success")
    assert rule.matches(_make_alert("etl", "low_success")) is True
    assert rule.matches(_make_alert("etl", "other")) is False
    assert rule.matches(_make_alert("other", "low_success")) is False


def test_suppress_alerts_splits_correctly():
    rules = [SuppressionRule(pipeline="etl", alert_type="low_success")]
    alerts = [
        _make_alert("etl", "low_success"),
        _make_alert("etl", "consecutive_failures"),
        _make_alert("other", "low_success"),
    ]
    active, suppressed = suppress_alerts(alerts, rules)
    assert len(suppressed) == 1
    assert suppressed[0].pipeline == "etl"
    assert suppressed[0].rule.name == "low_success"
    assert len(active) == 2


def test_suppress_alerts_no_rules():
    alerts = [_make_alert("etl", "low_success")]
    active, suppressed = suppress_alerts(alerts, [])
    assert active == alerts
    assert suppressed == []


def test_rule_str_representation():
    rule = SuppressionRule(pipeline="etl", alert_type="low_success", reason="ok")
    s = str(rule)
    assert "etl" in s
    assert "low_success" in s
