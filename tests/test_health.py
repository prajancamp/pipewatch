import pytest
from datetime import datetime, timezone
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.health import assess_health, overall_level, HealthStatus


def make_run(pipeline: str, status: PipelineStatus, idx: int = 0) -> PipelineRun:
    return PipelineRun(
        run_id=f"{pipeline}-{idx}",
        pipeline=pipeline,
        status=status,
        started_at=datetime(2024, 1, 1, idx, 0, 0, tzinfo=timezone.utc),
        ended_at=datetime(2024, 1, 1, idx, 0, 30, tzinfo=timezone.utc),
    )


def test_all_ok():
    runs = [make_run("etl", PipelineStatus.SUCCESS, i) for i in range(5)]
    statuses = assess_health(runs)
    assert len(statuses) == 1
    assert statuses[0].level == "ok"
    assert statuses[0].pipeline == "etl"


def test_warn_on_low_success_rate():
    runs = [
        make_run("etl", PipelineStatus.SUCCESS, 0),
        make_run("etl", PipelineStatus.FAILED, 1),
        make_run("etl", PipelineStatus.FAILED, 2),
        make_run("etl", PipelineStatus.FAILED, 3),
    ]
    statuses = assess_health(runs, warn_threshold=0.8, critical_threshold=0.4)
    assert statuses[0].level == "warn"


def test_critical_on_very_low_success_rate():
    runs = [make_run("etl", PipelineStatus.FAILED, i) for i in range(5)]
    statuses = assess_health(runs)
    assert statuses[0].level == "critical"


def test_warn_on_consecutive_failures():
    runs = [
        make_run("etl", PipelineStatus.SUCCESS, 0),
        make_run("etl", PipelineStatus.SUCCESS, 1),
        make_run("etl", PipelineStatus.SUCCESS, 2),
        make_run("etl", PipelineStatus.FAILED, 3),
        make_run("etl", PipelineStatus.FAILED, 4),
    ]
    statuses = assess_health(runs, consecutive_fail_warn=2, consecutive_fail_critical=4)
    assert statuses[0].level == "warn"


def test_multiple_pipelines_sorted():
    runs = (
        [make_run("good", PipelineStatus.SUCCESS, i) for i in range(4)]
        + [make_run("bad", PipelineStatus.FAILED, i) for i in range(4)]
    )
    statuses = assess_health(runs)
    levels = [s.level for s in statuses]
    assert levels[0] == "critical"
    assert levels[-1] == "ok"


def test_overall_level_critical():
    s = [HealthStatus("a", "ok", ""), HealthStatus("b", "critical", "")]
    assert overall_level(s) == "critical"


def test_overall_level_warn():
    s = [HealthStatus("a", "ok", ""), HealthStatus("b", "warn", "")]
    assert overall_level(s) == "warn"


def test_overall_level_ok():
    s = [HealthStatus("a", "ok", "")]
    assert overall_level(s) == "ok"


def test_str_representation():
    s = HealthStatus("pipe", "ok", "success rate 100%")
    assert "pipe" in str(s)
    assert "✅" in str(s)
