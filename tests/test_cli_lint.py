"""Tests for pipewatch.cli_lint."""
import argparse
import pytest
from pathlib import Path
from datetime import datetime, timezone
from pipewatch.store import RunStore
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.cli_lint import cmd_lint, register_lint_subcommands


@pytest.fixture
def store_path(tmp_path):
    return str(tmp_path / "runs")


def make_run(run_id="r1", pipeline="etl", status=PipelineStatus.SUCCESS, error=None):
    return PipelineRun(
        run_id=run_id,
        pipeline_name=pipeline,
        status=status,
        started_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
        finished_at=datetime(2024, 1, 1, 10, 5, tzinfo=timezone.utc),
        error=error,
        tags=[],
        meta={},
    )


def make_args(store, pipeline=None):
    return argparse.Namespace(store=store, pipeline=pipeline)


def test_lint_clean_store_exits_ok(store_path, capsys):
    s = RunStore(store_path)
    s.append(make_run())
    cmd_lint(make_args(store_path))
    out = capsys.readouterr().out
    assert "No lint issues" in out


def test_lint_detects_failed_no_error(store_path, capsys):
    s = RunStore(store_path)
    s.append(make_run(status=PipelineStatus.FAILED, error=None))
    with pytest.raises(SystemExit):
        cmd_lint(make_args(store_path))
    out = capsys.readouterr().out
    assert "FAILED_NO_ERROR" in out


def test_lint_filter_by_pipeline(store_path, capsys):
    s = RunStore(store_path)
    s.append(make_run(run_id="r1", pipeline="etl", status=PipelineStatus.SUCCESS))
    s.append(make_run(run_id="r2", pipeline="ingest", status=PipelineStatus.FAILED, error=None))
    cmd_lint(make_args(store_path, pipeline="etl"))
    out = capsys.readouterr().out
    assert "No lint issues" in out


def test_register_lint_subcommands():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    register_lint_subcommands(sub)
    args = parser.parse_args(["lint", "--store", "/tmp/x"])
    assert args.func == cmd_lint
    assert args.store == "/tmp/x"
