"""Tests for pipewatch.escalation."""
from __future__ import annotations

import pytest
from pipewatch.alert import Alert
from pipewatch.escalation import (
    EscalationResult,
    _escalation_level,
    _suggestion,
    escalate_alerts,
)


def make_alert(pipeline: str, rule: str = "low_success_rate") -> Alert:
    return Alert(pipeline=pipeline, rule=rule, message=f"{rule} triggered")


def test_escalation_level_low():
    assert _escalation_level(1) == "low"
    assert _escalation_level(2) == "low"


def test_escalation_level_medium():
    assert _escalation_level(3) == "medium"
    assert _escalation_level(4) == "medium"


def test_escalation_level_high():
    assert _escalation_level(5) == "high"
    assert _escalation_level(10) == "high"


def test_suggestion_returns_string_for_all_levels():
    for level in ("low", "medium", "high"):
        s = _suggestion(level)
        assert isinstance(s, str) and len(s) > 0


def test_suggestion_unknown_level_returns_none():
    assert _suggestion("critical") is None


def test_escalate_alerts_empty():
    results = escalate_alerts([])
    assert results == []


def test_escalate_alerts_single_pipeline_low():
    alerts = [make_alert("pipe_a")]
    results = escalate_alerts(alerts)
    assert len(results) == 1
    r = results[0]
    assert r.pipeline == "pipe_a"
    assert r.alert_count == 1
    assert r.level == "low"
    assert r.suggestion is not None


def test_escalate_alerts_medium():
    alerts = [make_alert("pipe_b", rule=f"rule_{i}") for i in range(3)]
    results = escalate_alerts(alerts)
    assert len(results) == 1
    assert results[0].level == "medium"


def test_escalate_alerts_high():
    alerts = [make_alert("pipe_c", rule=f"rule_{i}") for i in range(5)]
    results = escalate_alerts(alerts)
    assert results[0].level == "high"


def test_escalate_alerts_multiple_pipelines():
    alerts = [
        make_alert("alpha"),
        make_alert("beta"),
        make_alert("beta"),
        make_alert("beta"),
    ]
    results = escalate_alerts(alerts)
    by_name = {r.pipeline: r for r in results}
    assert by_name["alpha"].level == "low"
    assert by_name["beta"].level == "medium"


def test_escalation_result_str_contains_pipeline():
    r = EscalationResult(
        pipeline="my_pipe",
        alert_count=2,
        level="low",
        alerts=[make_alert("my_pipe")],
        suggestion="Monitor closely.",
    )
    text = str(r)
    assert "my_pipe" in text
    assert "LOW" in text
    assert "Monitor closely." in text
