"""Tests for pipewatch.badge."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.badge import (
    BadgeResult,
    _color_for_rate,
    generate_badge,
    generate_all_badges,
)


def make_run(
    pipeline: str = "etl",
    status: PipelineStatus = PipelineStatus.SUCCESS,
) -> PipelineRun:
    now = datetime.now(timezone.utc).isoformat()
    return PipelineRun(
        run_id=f"r-{pipeline}-{status.value}",
        pipeline=pipeline,
        status=status,
        started_at=now,
        ended_at=now,
    )


# --- _color_for_rate ---

def test_color_bright_green():
    assert _color_for_rate(1.0) == "brightgreen"
    assert _color_for_rate(0.95) == "brightgreen"


def test_color_yellow():
    assert _color_for_rate(0.90) == "yellow"
    assert _color_for_rate(0.80) == "yellow"


def test_color_orange():
    assert _color_for_rate(0.75) == "orange"
    assert _color_for_rate(0.60) == "orange"


def test_color_red():
    assert _color_for_rate(0.59) == "red"
    assert _color_for_rate(0.0) == "red"


# --- generate_badge ---

def test_badge_no_runs():
    badge = generate_badge("missing", [])
    assert badge.color == "lightgrey"
    assert badge.message == "no data"


def test_badge_all_success():
    runs = [make_run("etl", PipelineStatus.SUCCESS) for _ in range(5)]
    badge = generate_badge("etl", runs)
    assert badge.color == "brightgreen"
    assert badge.message == "100%"
    assert badge.pipeline == "etl"


def test_badge_all_failed():
    runs = [make_run("etl", PipelineStatus.FAILED) for _ in range(4)]
    badge = generate_badge("etl", runs)
    assert badge.color == "red"
    assert badge.message == "0%"


def test_badge_mixed():
    runs = (
        [make_run("etl", PipelineStatus.SUCCESS)] * 8
        + [make_run("etl", PipelineStatus.FAILED)] * 2
    )
    badge = generate_badge("etl", runs)
    assert badge.message == "80%"
    assert badge.color == "yellow"


# --- generate_all_badges ---

def test_generate_all_badges_multiple_pipelines():
    runs = [
        make_run("alpha", PipelineStatus.SUCCESS),
        make_run("beta", PipelineStatus.FAILED),
    ]
    badges = generate_all_badges(runs)
    pipelines = [b.pipeline for b in badges]
    assert "alpha" in pipelines
    assert "beta" in pipelines
    assert len(badges) == 2


def test_generate_all_badges_empty():
    assert generate_all_badges([]) == []


# --- to_shields_url ---

def test_shields_url_format():
    badge = BadgeResult(
        pipeline="etl", label="pipeline", message="95%", color="brightgreen"
    )
    url = badge.to_shields_url()
    assert url.startswith("https://img.shields.io/badge/")
    assert "brightgreen" in url
    assert "95%" in url or "95%25" in url or "95%" in url
