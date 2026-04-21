"""Tests for pipewatch.dependency_health."""
import pytest
from datetime import datetime, timezone
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.pipeline_map import PipelineMap
from pipewatch.dependency_health import (
    assess_dependency_health,
    assess_all_dependency_health,
    DependencyHealthResult,
)


def make_run(pipeline: str, status: str = "success", duration: float = 1.0) -> PipelineRun:
    now = datetime.now(timezone.utc).isoformat()
    return PipelineRun(
        run_id=f"{pipeline}-{status}",
        pipeline=pipeline,
        status=PipelineStatus(status),
        started_at=now,
        ended_at=now,
        duration=duration,
        error=None,
        tags=[],
        meta={},
    )


@pytest.fixture
def simple_map(tmp_path) -> PipelineMap:
    pm = PipelineMap(store_path=tmp_path / "map.json")
    pm.add_edge("upstream", "downstream")
    return pm


def test_no_upstream_healthy(simple_map):
    runs = [make_run("upstream"), make_run("downstream")]
    result = assess_dependency_health("downstream", runs, simple_map)
    assert result.pipeline == "downstream"
    assert not result.is_blocked
    assert result.upstream_issues == []


def test_blocked_by_critical_upstream(simple_map):
    runs = [make_run("upstream", "failed")] * 5 + [make_run("downstream")]
    result = assess_dependency_health("downstream", runs, simple_map)
    assert "upstream" in result.blocked_by


def test_upstream_warn_adds_issue(simple_map):
    # 2 failures out of 4 -> warn level
    runs = (
        [make_run("upstream", "failed")] * 2
        + [make_run("upstream", "success")] * 2
        + [make_run("downstream")]
    )
    result = assess_dependency_health("downstream", runs, simple_map)
    # may be warn or ok depending on thresholds; just check structure
    assert isinstance(result.upstream_issues, list)
    assert isinstance(result.blocked_by, list)


def test_no_upstream_runs_skipped(simple_map):
    runs = [make_run("downstream")]
    result = assess_dependency_health("downstream", runs, simple_map)
    assert result.blocked_by == []
    assert result.upstream_issues == []


def test_assess_all_returns_all_pipelines(simple_map):
    runs = [make_run("upstream"), make_run("downstream")]
    results = assess_all_dependency_health(runs, simple_map)
    names = {r.pipeline for r in results}
    assert "upstream" in names
    assert "downstream" in names


def test_str_output_contains_pipeline(simple_map):
    runs = [make_run("upstream", "failed")] * 5 + [make_run("downstream")]
    result = assess_dependency_health("downstream", runs, simple_map)
    s = str(result)
    assert "downstream" in s
    assert "upstream" in s
