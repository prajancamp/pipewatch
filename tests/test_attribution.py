"""Tests for pipewatch.attribution."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest

from pipewatch.attribution import (
    AttributionEntry,
    attribute_runs,
    attribution_by_team,
)
from pipewatch.models import PipelineRun, PipelineStatus


def make_run(
    pipeline: str,
    status: PipelineStatus = PipelineStatus.SUCCESS,
    owner: Optional[str] = None,
    team: Optional[str] = None,
) -> PipelineRun:
    meta = {}
    if owner:
        meta["owner"] = owner
    if team:
        meta["team"] = team
    return PipelineRun(
        run_id=f"{pipeline}-{status.value}",
        pipeline=pipeline,
        status=status,
        started_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).isoformat(),
        ended_at=datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc).isoformat(),
        meta=meta or None,
    )


def test_attribute_runs_basic():
    runs = [
        make_run("etl_users", PipelineStatus.SUCCESS, owner="alice", team="data"),
        make_run("etl_users", PipelineStatus.FAILED, owner="alice", team="data"),
    ]
    entries = attribute_runs(runs)
    assert len(entries) == 1
    e = entries[0]
    assert e.pipeline == "etl_users"
    assert e.owner == "alice"
    assert e.team == "data"
    assert e.total_runs == 2
    assert e.failed_runs == 1
    assert e.success_rate == pytest.approx(0.5)


def test_attribute_runs_multiple_pipelines():
    runs = [
        make_run("pipe_a", PipelineStatus.SUCCESS, team="alpha"),
        make_run("pipe_b", PipelineStatus.FAILED, team="beta"),
        make_run("pipe_b", PipelineStatus.SUCCESS, team="beta"),
    ]
    entries = attribute_runs(runs)
    assert len(entries) == 2
    names = [e.pipeline for e in entries]
    assert "pipe_a" in names
    assert "pipe_b" in names


def test_attribute_runs_filter_pipeline():
    runs = [
        make_run("pipe_a", PipelineStatus.SUCCESS, owner="alice"),
        make_run("pipe_b", PipelineStatus.SUCCESS, owner="bob"),
    ]
    entries = attribute_runs(runs, pipeline="pipe_a")
    assert len(entries) == 1
    assert entries[0].pipeline == "pipe_a"


def test_attribute_runs_no_meta():
    runs = [make_run("bare_pipe")]
    entries = attribute_runs(runs)
    assert entries[0].owner is None
    assert entries[0].team is None


def test_attribute_runs_empty():
    assert attribute_runs([]) == []


def test_attribution_by_team_groups_correctly():
    entries = [
        AttributionEntry("p1", "alice", "data", 10, 1, 0.9),
        AttributionEntry("p2", "bob", "data", 5, 0, 1.0),
        AttributionEntry("p3", "carol", "infra", 8, 2, 0.75),
    ]
    grouped = attribution_by_team(entries)
    assert set(grouped.keys()) == {"data", "infra"}
    assert len(grouped["data"]) == 2
    assert len(grouped["infra"]) == 1


def test_attribution_by_team_unknown_fallback():
    entries = [
        AttributionEntry("p1", None, None, 3, 0, 1.0),
    ]
    grouped = attribution_by_team(entries)
    assert "unknown" in grouped


def test_attribution_entry_str():
    e = AttributionEntry("my_pipe", "alice", "data", 10, 2, 0.8)
    s = str(e)
    assert "my_pipe" in s
    assert "alice" in s
    assert "data" in s
