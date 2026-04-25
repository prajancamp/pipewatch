"""Tests for pipewatch.burndown."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.burndown import compute_burndown, BurndownReport
from pipewatch.models import PipelineRun, PipelineStatus


def make_run(
    pipeline: str,
    status: PipelineStatus,
    started_offset: int = 0,
) -> PipelineRun:
    started = datetime(2024, 1, 1, 12, started_offset, 0, tzinfo=timezone.utc)
    ended = datetime(2024, 1, 1, 12, started_offset + 1, 0, tzinfo=timezone.utc)
    return PipelineRun(
        run_id=f"{pipeline}-{started_offset}",
        pipeline=pipeline,
        status=status,
        started_at=started,
        ended_at=ended,
    )


def test_compute_burndown_empty():
    report = compute_burndown([])
    assert report.total_opened == 0
    assert report.total_resolved == 0
    assert report.resolution_rate == 1.0
    assert report.points == []


def test_compute_burndown_all_success():
    runs = [make_run("pipe-a", PipelineStatus.SUCCESS, i) for i in range(3)]
    report = compute_burndown(runs)
    assert report.total_opened == 0
    assert report.total_resolved == 0
    assert report.resolution_rate == 1.0


def test_compute_burndown_failure_opens_incident():
    runs = [
        make_run("pipe-a", PipelineStatus.FAILED, 0),
        make_run("pipe-a", PipelineStatus.FAILED, 1),
    ]
    report = compute_burndown(runs)
    assert report.total_opened == 2
    assert report.total_resolved == 0
    assert report.resolution_rate == 0.0


def test_compute_burndown_success_resolves_failure():
    runs = [
        make_run("pipe-a", PipelineStatus.FAILED, 0),
        make_run("pipe-a", PipelineStatus.SUCCESS, 1),
    ]
    report = compute_burndown(runs)
    assert report.total_opened == 1
    assert report.total_resolved == 1
    assert report.resolution_rate == 1.0


def test_compute_burndown_partial_resolution():
    runs = [
        make_run("pipe-a", PipelineStatus.FAILED, 0),
        make_run("pipe-a", PipelineStatus.FAILED, 1),
        make_run("pipe-a", PipelineStatus.SUCCESS, 2),
    ]
    report = compute_burndown(runs)
    assert report.total_opened == 2
    assert report.total_resolved == 1
    assert report.resolution_rate == 0.5


def test_compute_burndown_filter_pipeline():
    runs = [
        make_run("pipe-a", PipelineStatus.FAILED, 0),
        make_run("pipe-b", PipelineStatus.FAILED, 1),
        make_run("pipe-a", PipelineStatus.SUCCESS, 2),
    ]
    report = compute_burndown(runs, pipeline="pipe-a")
    assert report.total_opened == 1
    assert report.total_resolved == 1
    assert all(pt.pipeline is None for pt in report.points)


def test_compute_burndown_multiple_pipelines_independent():
    runs = [
        make_run("pipe-a", PipelineStatus.FAILED, 0),
        make_run("pipe-b", PipelineStatus.FAILED, 1),
        make_run("pipe-a", PipelineStatus.SUCCESS, 2),
    ]
    report = compute_burndown(runs)
    assert report.total_opened == 2
    assert report.total_resolved == 1
    # After last point, pipe-b still open
    assert report.points[-1].open_failures == 1


def test_burndown_str_output():
    runs = [
        make_run("pipe-a", PipelineStatus.FAILED, 0),
        make_run("pipe-a", PipelineStatus.SUCCESS, 1),
    ]
    report = compute_burndown(runs, pipeline="pipe-a")
    text = str(report)
    assert "pipe-a" in text
    assert "Opened" in text
    assert "Resolved" in text
    assert "Resolution rate" in text
