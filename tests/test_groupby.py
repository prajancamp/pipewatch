from datetime import datetime, timezone
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.groupby import (
    group_by_status,
    group_by_pipeline,
    group_by_date,
    group_by_meta_field,
    counts,
)


def make_run(name="pipe", status=PipelineStatus.SUCCESS, start="2024-01-15T10:00:00", meta=None):
    return PipelineRun(
        run_id=f"{name}-{status.value}-{start}",
        pipeline_name=name,
        status=status,
        start_time=datetime.fromisoformat(start).replace(tzinfo=timezone.utc),
        end_time=datetime.fromisoformat(start).replace(tzinfo=timezone.utc),
        meta=meta or {},
    )


def test_group_by_status_basic():
    runs = [
        make_run(status=PipelineStatus.SUCCESS),
        make_run(status=PipelineStatus.FAILED),
        make_run(status=PipelineStatus.SUCCESS),
    ]
    groups = group_by_status(runs)
    assert len(groups["success"]) == 2
    assert len(groups["failed"]) == 1


def test_group_by_status_empty():
    assert group_by_status([]) == {}


def test_group_by_pipeline():
    runs = [
        make_run(name="alpha"),
        make_run(name="beta"),
        make_run(name="alpha"),
    ]
    groups = group_by_pipeline(runs)
    assert set(groups.keys()) == {"alpha", "beta"}
    assert len(groups["alpha"]) == 2


def test_group_by_date():
    runs = [
        make_run(start="2024-01-15T08:00:00"),
        make_run(start="2024-01-15T20:00:00"),
        make_run(start="2024-01-16T08:00:00"),
    ]
    groups = group_by_date(runs)
    assert len(groups["2024-01-15"]) == 2
    assert len(groups["2024-01-16"]) == 1


def test_group_by_date_hourly():
    runs = [
        make_run(start="2024-01-15T08:00:00"),
        make_run(start="2024-01-15T08:30:00"),
        make_run(start="2024-01-15T09:00:00"),
    ]
    groups = group_by_date(runs, fmt="%Y-%m-%dT%H")
    assert len(groups["2024-01-15T08"]) == 2
    assert len(groups["2024-01-15T09"]) == 1


def test_group_by_meta_field():
    runs = [
        make_run(meta={"env": "prod"}),
        make_run(meta={"env": "staging"}),
        make_run(meta={"env": "prod"}),
        make_run(meta={}),
    ]
    groups = group_by_meta_field(runs, "env")
    assert len(groups["prod"]) == 2
    assert len(groups["staging"]) == 1
    assert len(groups["__missing__"]) == 1


def test_counts():
    runs = [
        make_run(name="a"),
        make_run(name="a"),
        make_run(name="b"),
    ]
    groups = group_by_pipeline(runs)
    c = counts(groups)
    assert c == {"a": 2, "b": 1}
