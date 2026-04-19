"""Tests for pipewatch.cli_rollup."""
from __future__ import annotations
import argparse
import time
from pathlib import Path
from datetime import datetime, timezone
from pipewatch.store import RunStore
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.cli_rollup import cmd_rollup, register_rollup_subcommands


DAY1 = datetime(2024, 3, 5, 8, 0, 0, tzinfo=timezone.utc).timestamp()


def seed(path: Path):
    store = RunStore(path)
    for i, status in enumerate([PipelineStatus.SUCCESS, PipelineStatus.FAILED]):
        store.append(
            PipelineRun(
                run_id=f"r{i}",
                pipeline="ingest",
                status=status,
                started_at=DAY1 + i,
                duration=5.0,
            )
        )


def make_args(store_path: Path, **kwargs) -> argparse.Namespace:
    defaults = {"store": store_path, "granularity": "daily", "pipeline": None}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_rollup_empty_store(tmp_path, capsys):
    args = make_args(tmp_path / "store.jsonl")
    cmd_rollup(args)
    assert "No runs" in capsys.readouterr().out


def test_rollup_shows_period(tmp_path, capsys):
    p = tmp_path / "store.jsonl"
    seed(p)
    args = make_args(p)
    cmd_rollup(args)
    out = capsys.readouterr().out
    assert "2024-03-05" in out
    assert "ingest" in out


def test_rollup_filter_pipeline(tmp_path, capsys):
    p = tmp_path / "store.jsonl"
    seed(p)
    store = RunStore(p)
    store.append(
        PipelineRun(
            run_id="other", pipeline="other", status=PipelineStatus.SUCCESS,
            started_at=DAY1, duration=1.0,
        )
    )
    args = make_args(p, pipeline="ingest")
    cmd_rollup(args)
    out = capsys.readouterr().out
    assert "other" not in out
    assert "ingest" in out


def test_register_rollup_subcommands():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    register_rollup_subcommands(sub)
    args = parser.parse_args(["rollup", "--granularity", "hourly"])
    assert args.granularity == "hourly"
