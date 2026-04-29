"""Tests for pipewatch.cli_quorum."""
from __future__ import annotations

import argparse
import pytest
from pathlib import Path

from pipewatch.store import RunStore
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.cli_quorum import cmd_quorum, register_quorum_subcommands


def make_run(
    pipeline: str,
    status: str,
    started_at: str,
) -> PipelineRun:
    return PipelineRun(
        run_id=f"{pipeline}-{started_at}-{status}",
        pipeline=pipeline,
        status=PipelineStatus(status),
        started_at=started_at,
        ended_at=started_at,
    )


def store_path(tmp_path: Path) -> Path:
    return tmp_path / "runs.jsonl"


def make_args(store: Path, pipeline=None, window=5, required=3) -> argparse.Namespace:
    return argparse.Namespace(
        store=store,
        pipeline=pipeline,
        window=window,
        required=required,
    )


def seed(path: Path, runs) -> None:
    s = RunStore(path)
    for r in runs:
        s.append(r)


def test_quorum_empty_store(tmp_path, capsys):
    p = store_path(tmp_path)
    cmd_quorum(make_args(p))
    out = capsys.readouterr().out
    assert "No pipeline runs" in out


def test_quorum_confident_success(tmp_path, capsys):
    p = store_path(tmp_path)
    runs = [
        make_run("pipe", "success", f"2024-01-0{i}T00:00:00") for i in range(1, 6)
    ]
    seed(p, runs)
    cmd_quorum(make_args(p, window=5, required=3))
    out = capsys.readouterr().out
    assert "success" in out
    assert "[confident]" in out


def test_quorum_undecided(tmp_path, capsys):
    p = store_path(tmp_path)
    runs = [
        make_run("pipe", "failed", "2024-01-01T00:00:00"),
        make_run("pipe", "success", "2024-01-02T00:00:00"),
    ]
    seed(p, runs)
    cmd_quorum(make_args(p, window=5, required=3))
    out = capsys.readouterr().out
    assert "[undecided]" in out


def test_quorum_filter_pipeline(tmp_path, capsys):
    p = store_path(tmp_path)
    runs = [
        make_run("alpha", "success", f"2024-01-0{i}T00:00:00") for i in range(1, 4)
    ] + [
        make_run("beta", "failed", f"2024-01-0{i}T00:00:00") for i in range(1, 4)
    ]
    seed(p, runs)
    cmd_quorum(make_args(p, pipeline="alpha", window=5, required=3))
    out = capsys.readouterr().out
    assert "alpha" in out
    assert "beta" not in out


def test_register_quorum_subcommands():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    register_quorum_subcommands(sub)
    args = parser.parse_args(["quorum", "--window", "4", "--required", "2"])
    assert args.window == 4
    assert args.required == 2
