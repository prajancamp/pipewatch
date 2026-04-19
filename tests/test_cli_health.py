import argparse
import pytest
from pathlib import Path
from datetime import datetime, timezone
from pipewatch.store import RunStore
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.cli_health import cmd_health, register_health_subcommands


@pytest.fixture
def store_path(tmp_path):
    return tmp_path / "runs.jsonl"


def make_args(store_path, **kwargs):
    defaults = dict(
        store=str(store_path),
        warn_rate=0.8,
        critical_rate=0.5,
        warn_consec=2,
        critical_consec=4,
        exit_code=False,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def seed(store_path, pipeline, statuses):
    store = RunStore(str(store_path))
    for i, st in enumerate(statuses):
        run = PipelineRun(
            run_id=f"{pipeline}-{i}",
            pipeline=pipeline,
            status=st,
            started_at=datetime(2024, 1, 1, i, 0, 0, tzinfo=timezone.utc),
            ended_at=datetime(2024, 1, 1, i, 0, 10, tzinfo=timezone.utc),
        )
        store.append(run)


def test_health_empty_store(store_path, capsys):
    cmd_health(make_args(store_path))
    out = capsys.readouterr().out
    assert "No pipeline runs found" in out


def test_health_ok_pipeline(store_path, capsys):
    seed(store_path, "pipe", [PipelineStatus.SUCCESS] * 5)
    cmd_health(make_args(store_path))
    out = capsys.readouterr().out
    assert "ok" in out.lower()
    assert "pipe" in out


def test_health_critical_pipeline(store_path, capsys):
    seed(store_path, "pipe", [PipelineStatus.FAILED] * 5)
    cmd_health(make_args(store_path))
    out = capsys.readouterr().out
    assert "critical" in out.lower()


def test_exit_code_critical(store_path):
    seed(store_path, "pipe", [PipelineStatus.FAILED] * 5)
    with pytest.raises(SystemExit) as exc:
        cmd_health(make_args(store_path, exit_code=True))
    assert exc.value.code == 2


def test_exit_code_ok_no_exit(store_path):
    seed(store_path, "pipe", [PipelineStatus.SUCCESS] * 5)
    cmd_health(make_args(store_path, exit_code=True))  # should not raise


def test_register_subcommands():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    register_health_subcommands(sub)
    args = parser.parse_args(["health", "--store", "x.jsonl"])
    assert args.store == "x.jsonl"
    assert args.warn_rate == 0.8
