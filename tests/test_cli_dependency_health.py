"""Tests for pipewatch.cli_dependency_health."""
import argparse
import pytest
from pathlib import Path
from datetime import datetime, timezone
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore
from pipewatch.pipeline_map import PipelineMap
from pipewatch.cli_dependency_health import cmd_dependency_health


def make_run(pipeline: str, status: str = "success") -> PipelineRun:
    now = datetime.now(timezone.utc).isoformat()
    return PipelineRun(
        run_id=f"{pipeline}-{status}-{id(object())}",
        pipeline=pipeline,
        status=PipelineStatus(status),
        started_at=now,
        ended_at=now,
        duration=1.0,
        error=None,
        tags=[],
        meta={},
    )


@pytest.fixture
def store_path(tmp_path) -> Path:
    return tmp_path / "runs.jsonl"


def make_args(store_path: Path, pipeline=None) -> argparse.Namespace:
    return argparse.Namespace(store=store_path, pipeline=pipeline)


def seed(store_path, runs):
    s = RunStore(store_path)
    for r in runs:
        s.append(r)


def make_map(store_path: Path) -> PipelineMap:
    return PipelineMap(store_path=store_path.parent / "pipeline_map.json")


def test_dep_health_no_map_prints_message(store_path, capsys):
    seed(store_path, [make_run("pipe_a")])
    cmd_dependency_health(make_args(store_path))
    out = capsys.readouterr().out
    assert "No pipeline map" in out


def test_dep_health_all_ok(store_path, capsys):
    pm = make_map(store_path)
    pm.add_edge("upstream", "downstream")
    seed(store_path, [make_run("upstream")] * 3 + [make_run("downstream")] * 3)
    cmd_dependency_health(make_args(store_path))
    out = capsys.readouterr().out
    assert "upstream" in out
    assert "downstream" in out


def test_dep_health_blocked_exits_nonzero(store_path):
    pm = make_map(store_path)
    pm.add_edge("upstream", "downstream")
    seed(store_path, [make_run("upstream", "failed")] * 6 + [make_run("downstream")] * 3)
    with pytest.raises(SystemExit) as exc:
        cmd_dependency_health(make_args(store_path))
    assert exc.value.code == 1


def test_dep_health_single_pipeline(store_path, capsys):
    pm = make_map(store_path)
    pm.add_edge("upstream", "downstream")
    seed(store_path, [make_run("upstream")] * 3 + [make_run("downstream")] * 3)
    cmd_dependency_health(make_args(store_path, pipeline="downstream"))
    out = capsys.readouterr().out
    assert "downstream" in out
