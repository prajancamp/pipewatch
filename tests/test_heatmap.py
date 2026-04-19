"""Tests for pipewatch.heatmap"""
from datetime import datetime, timezone
import pytest
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.heatmap import compute_heatmap, top_failure_slots, HeatmapCell


def make_run(pipeline="pipe", status=PipelineStatus.SUCCESS, hour=10, weekday=0):
    # weekday 0 = Monday; build a date that lands on the right weekday
    from datetime import timedelta
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)  # Monday
    delta_days = (weekday - base.weekday()) % 7
    dt = base + timedelta(days=delta_days, hours=hour - base.hour)
    return PipelineRun(
        run_id=f"{pipeline}-{weekday}-{hour}-{status.value}",
        pipeline=pipeline,
        status=status,
        started_at=dt,
        ended_at=dt,
    )


def test_compute_heatmap_empty():
    assert compute_heatmap([]) == []


def test_compute_heatmap_single_success():
    runs = [make_run(status=PipelineStatus.SUCCESS, hour=9, weekday=0)]
    cells = compute_heatmap(runs)
    assert len(cells) == 1
    assert cells[0].total == 1
    assert cells[0].failures == 0
    assert cells[0].failure_rate == 0.0


def test_compute_heatmap_counts_failures():
    runs = [
        make_run(status=PipelineStatus.FAILED, hour=9, weekday=1),
        make_run(status=PipelineStatus.FAILED, hour=9, weekday=1),
        make_run(status=PipelineStatus.SUCCESS, hour=9, weekday=1),
    ]
    cells = compute_heatmap(runs)
    assert len(cells) == 1
    assert cells[0].failures == 2
    assert cells[0].total == 3
    assert abs(cells[0].failure_rate - 2 / 3) < 1e-6


def test_compute_heatmap_filter_pipeline():
    runs = [
        make_run(pipeline="a", status=PipelineStatus.FAILED, hour=8, weekday=0),
        make_run(pipeline="b", status=PipelineStatus.FAILED, hour=8, weekday=0),
    ]
    cells = compute_heatmap(runs, pipeline="a")
    assert all(True for c in cells)  # just one pipeline worth
    total_runs = sum(c.total for c in cells)
    assert total_runs == 1


def test_compute_heatmap_skips_no_started_at():
    run = PipelineRun(
        run_id="x", pipeline="p", status=PipelineStatus.FAILED,
        started_at=None, ended_at=None,
    )
    cells = compute_heatmap([run])
    assert cells == []


def test_top_failure_slots():
    cells = [
        HeatmapCell(day="Mon", hour=1, total=10, failures=1),
        HeatmapCell(day="Tue", hour=2, total=10, failures=5),
        HeatmapCell(day="Wed", hour=3, total=10, failures=3),
    ]
    top = top_failure_slots(cells, n=2)
    assert len(top) == 2
    assert top[0].failures == 5
    assert top[1].failures == 3


def test_heatmap_cell_str():
    cell = HeatmapCell(day="Mon", hour=9, total=4, failures=2)
    s = str(cell)
    assert "Mon" in s
    assert "50%" in s
