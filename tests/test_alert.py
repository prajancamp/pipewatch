"""Tests for pipewatch.alert module."""
import pytest
from datetime import datetime, timezone
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.analyzer import PipelineStats, compute_stats
from pipewatch.alert import AlertRule, Alert, evaluate_alerts


def make_run(pipeline="etl", status=PipelineStatus.SUCCESS, duration=10.0):
    return PipelineRun(
        pipeline=pipeline,
        status=status,
        started_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ended_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        duration=duration,
    )


def test_no_alerts_healthy():
    runs = [make_run(status=PipelineStatus.SUCCESS) for _ in range(5)]
    stats = compute_stats(runs)
    rule = AlertRule(consecutive_failures=3, min_success_rate=0.5)
    alerts = evaluate_alerts(stats, runs, rule)
    assert alerts == []


def test_alert_low_success_rate():
    runs = [make_run(status=PipelineStatus.FAILED) for _ in range(4)]
    runs += [make_run(status=PipelineStatus.SUCCESS)]
    stats = compute_stats(runs)
    rule = AlertRule(min_success_rate=0.8)
    alerts = evaluate_alerts(stats, runs, rule)
    assert any("Success rate" in a.message for a in alerts)
    assert any(a.level == "critical" for a in alerts)


def test_alert_consecutive_failures():
    runs = [make_run(status=PipelineStatus.FAILED) for _ in range(4)]
    stats = compute_stats(runs)
    rule = AlertRule(consecutive_failures=3, min_success_rate=0.0)
    alerts = evaluate_alerts(stats, runs, rule)
    assert any("consecutive failures" in a.message for a in alerts)


def test_alert_avg_duration():
    runs = [make_run(duration=200.0) for _ in range(3)]
    stats = compute_stats(runs)
    rule = AlertRule(min_success_rate=0.0, max_avg_duration=100.0)
    alerts = evaluate_alerts(stats, runs, rule)
    assert any("duration" in a.message for a in alerts)
    assert any(a.level == "warning" for a in alerts)


def test_alert_str_format():
    a = Alert(pipeline="etl", level="critical", message="bad")
    assert "CRITICAL" in str(a)
    assert "etl" in str(a)
    b = Alert(pipeline="etl", level="warning", message="slow")
    assert "WARNING" in str(b)
