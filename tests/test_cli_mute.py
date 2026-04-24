"""Tests for pipewatch.cli_mute subcommands."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipewatch.cli_mute import (
    cmd_mute_add,
    cmd_mute_check,
    cmd_mute_list,
    cmd_mute_prune,
    cmd_mute_remove,
)
from pipewatch.mute import MuteRule, add_mute_rule, load_mute_rules, save_mute_rules


@pytest.fixture
def store_path(tmp_path: Path) -> str:
    return str(tmp_path / "runs.jsonl")


def make_args(store_path: str, **kwargs) -> argparse.Namespace:
    defaults = {"store": store_path, "pipeline": "my_pipe", "reason": "test", "hours": None}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def _past_iso(hours: float = 2) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


def _future_iso(hours: float = 2) -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()


def test_mute_add_permanent(store_path, capsys):
    cmd_mute_add(make_args(store_path))
    rules = load_mute_rules(store_path)
    assert len(rules) == 1
    assert rules[0].pipeline == "my_pipe"
    assert rules[0].expires_at is None
    out = capsys.readouterr().out
    assert "Muted" in out


def test_mute_add_with_hours(store_path):
    cmd_mute_add(make_args(store_path, hours=4.0))
    rules = load_mute_rules(store_path)
    assert rules[0].expires_at is not None


def test_mute_list_empty(store_path, capsys):
    cmd_mute_list(make_args(store_path))
    out = capsys.readouterr().out
    assert "No active" in out


def test_mute_list_shows_active(store_path, capsys):
    rule = MuteRule(pipeline="etl", reason="maint", expires_at=_future_iso())
    add_mute_rule(store_path, rule)
    cmd_mute_list(make_args(store_path))
    out = capsys.readouterr().out
    assert "etl" in out


def test_mute_remove(store_path, capsys):
    add_mute_rule(store_path, MuteRule(pipeline="my_pipe", reason="r", expires_at=None))
    cmd_mute_remove(make_args(store_path))
    rules = load_mute_rules(store_path)
    assert all(r.pipeline != "my_pipe" for r in rules)
    out = capsys.readouterr().out
    assert "Removed 1" in out


def test_mute_prune(store_path, capsys):
    rules = [
        MuteRule(pipeline="a", reason="r", expires_at=_past_iso()),
        MuteRule(pipeline="b", reason="r", expires_at=_future_iso()),
    ]
    save_mute_rules(store_path, rules)
    cmd_mute_prune(make_args(store_path))
    out = capsys.readouterr().out
    assert "1" in out
    remaining = load_mute_rules(store_path)
    assert len(remaining) == 1


def test_mute_check_muted(store_path, capsys):
    add_mute_rule(store_path, MuteRule(pipeline="my_pipe", reason="r", expires_at=None))
    cmd_mute_check(make_args(store_path))
    out = capsys.readouterr().out
    assert "MUTED" in out


def test_mute_check_not_muted(store_path, capsys):
    cmd_mute_check(make_args(store_path))
    out = capsys.readouterr().out
    assert "NOT muted" in out
