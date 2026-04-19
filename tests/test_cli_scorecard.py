import argparse
import pytest
from unittest.mock import patch
from pipewatch.store import RunStore
from pipewatch.cli_scorecard import cmd_scorecard
from pipewatch.models import PipelineRun, PipelineStatus
from datetime import datetime, timezone


def make_run(pipeline, status, store_path):
    run = PipelineRun(
        run_id=f"{pipeline}-{status}-1",
        pipeline=pipeline,
        status=PipelineStatus(status),
        started_at=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        ended_at=datetime(2024, 1, 1, 10, 5, 0, tzinfo=timezone.utc),
    )
    RunStore(store_path).append(run)
    return run


@pytest.fixture
def store_path(tmp_path):
    return str(tmp_path / "runs.jsonl")


def make_args(store_path):
    args = argparse.Namespace()
    args.store = store_path
    return args


def test_scorecard_empty_store(store_path, capsys):
    cmd_scorecard(make_args(store_path))
    out = capsys.readouterr().out
    assert "No runs" in out


def test_scorecard_shows_pipeline(store_path, capsys):
    make_run("alpha", "success", store_path)
    make_run("alpha", "success", store_path)
    cmd_scorecard(make_args(store_path))
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "A" in out


def test_scorecard_failing_pipeline_low_grade(store_path, capsys):
    for _ in range(8):
        make_run("broken", "failed", store_path)
    make_run("broken", "success", store_path)
    cmd_scorecard(make_args(store_path))
    out = capsys.readouterr().out
    assert "broken" in out
    # Should not be an A
    assert "100.0" not in out
