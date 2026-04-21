"""Tests for pipewatch.pulse."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List
from unittest.mock import patch

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.pulse import (
    PulseResult,
    compute_all_pulses,
    compute_pulse,
    silent_pipelines,
)


def _dt(offset_hours: float = 0) -> datetime:
    """Return a UTC datetime offset_hours from now (negative = past)."""
    return datetime.now(tz=timezone.utc) + timedelta(hours=offset_hours)


def make_run(
    pipeline: str = "pipe_a",
    status: PipelineStatus = PipelineStatus.SUCCESS,
    started_offset_hours: float = -1,
) -> PipelineRun:
    started = _dt(started_offset_hours)
    finished = started + timedelta(seconds=30)
    return PipelineRun(
        run_id=f"{pipeline}-{started_offset_hours}",
        pipeline=pipeline,
        status=status,
        started_at=started,
        finished_at=finished,
    )


def test_pulse_active_pipeline():
    runs = [make_run("pipe_a", started_offset_hours=-0.5)]
    result = compute_pulse(runs, "pipe_a")
    assert result.is_active is True
    assert result.runs_last_hour == 1
    assert result.runs_last_day == 1
    assert result.last_seen is not None


def test_pulse_silent_pipeline():
    runs = [make_run("pipe_a", started_offset_hours=-25)]
    result = compute_pulse(runs, "pipe_a")
    assert result.is_active is False
    assert result.runs_last_day == 0
    assert result.runs_last_hour == 0


def test_pulse_no_runs_for_pipeline():
    result = compute_pulse([], "pipe_a")
    assert result.is_active is False
    assert result.last_seen is None
    assert result.runs_last_hour == 0


def test_runs_last_hour_excludes_older():
    runs = [
        make_run("pipe_a", started_offset_hours=-0.5),
        make_run("pipe_a", started_offset_hours=-2),
    ]
    result = compute_pulse(runs, "pipe_a")
    assert result.runs_last_hour == 1
    assert result.runs_last_day == 2


def test_compute_all_pulses_multiple_pipelines():
    runs = [
        make_run("pipe_a", started_offset_hours=-0.5),
        make_run("pipe_b", started_offset_hours=-25),
    ]
    pulses = compute_all_pulses(runs)
    assert set(pulses.keys()) == {"pipe_a", "pipe_b"}
    assert pulses["pipe_a"].is_active is True
    assert pulses["pipe_b"].is_active is False


def test_compute_all_pulses_empty():
    assert compute_all_pulses([]) == {}


def test_silent_pipelines_filters_correctly():
    runs = [
        make_run("pipe_a", started_offset_hours=-0.5),
        make_run("pipe_b", started_offset_hours=-25),
    ]
    pulses = compute_all_pulses(runs)
    silent = silent_pipelines(pulses)
    assert len(silent) == 1
    assert silent[0].pipeline == "pipe_b"


def test_pulse_result_str_active():
    runs = [make_run("pipe_a", started_offset_hours=-0.5)]
    result = compute_pulse(runs, "pipe_a")
    text = str(result)
    assert "ACTIVE" in text
    assert "pipe_a" in text


def test_pulse_result_str_silent():
    result = compute_pulse([], "pipe_z")
    text = str(result)
    assert "SILENT" in text
    assert "never" in text
