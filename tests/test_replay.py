"""Tests for pipewatch.replay."""
from __future__ import annotations
import pytest
from datetime import datetime
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.alert import AlertRule
from pipewatch.replay import replay_pipeline, replay_all


def make_run(pipeline: str, status: PipelineStatus, duration: float = 1.0) -> PipelineRun:
    return PipelineRun(
        run_id=f"{pipeline}-{status}-{duration}",
        pipeline=pipeline,
        status=status,
        started_at=datetime(2024, 1, 1, 12, 0, 0),
        duration_seconds=duration,
    )


def test_replay_pipeline_no_alerts():
    runs = [make_run("etl", PipelineStatus.SUCCESS) for _ in range(5)]
    rules = [AlertRule(min_success_rate=0.8)]
    result = replay_pipeline(runs, rules, "etl")
    assert result.pipeline == "etl"
    assert result.total_runs == 5
    assert result.alerts_fired == []


def test_replay_pipeline_triggers_alert():
    runs = [make_run("etl", PipelineStatus.FAILED) for _ in range(5)]
    rules = [AlertRule(min_success_rate=0.5)]
    result = replay_pipeline(runs, rules, "etl")
    assert len(result.alerts_fired) == 1


def test_replay_pipeline_filters_by_name():
    runs = [
        make_run("etl", PipelineStatus.FAILED),
        make_run("other", PipelineStatus.SUCCESS),
    ]
    rules = [AlertRule(min_success_rate=0.9)]
    result = replay_pipeline(runs, rules, "etl")
    assert result.total_runs == 1


def test_replay_all_returns_one_per_pipeline():
    runs = [
        make_run("a", PipelineStatus.SUCCESS),
        make_run("b", PipelineStatus.FAILED),
    ]
    rules = [AlertRule(min_success_rate=0.9)]
    results = replay_all(runs, rules)
    assert {r.pipeline for r in results} == {"a", "b"}


def test_replay_all_limited_to_specified_pipelines():
    runs = [
        make_run("a", PipelineStatus.SUCCESS),
        make_run("b", PipelineStatus.FAILED),
        make_run("c", PipelineStatus.SUCCESS),
    ]
    rules = [AlertRule(min_success_rate=0.5)]
    results = replay_all(runs, rules, pipelines=["a", "c"])
    assert [r.pipeline for r in results] == ["a", "c"]


def test_replay_result_str():
    runs = [make_run("etl", PipelineStatus.FAILED) for _ in range(3)]
    rules = [AlertRule(min_success_rate=0.9)]
    result = replay_pipeline(runs, rules, "etl")
    out = str(result)
    assert "etl" in out
    assert "3 run" in out
