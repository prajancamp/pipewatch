"""Tests for pipewatch.watermark."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.watermark import (
    WatermarkEntry,
    compute_watermarks,
    load_watermarks,
    save_watermarks,
    update_watermarks,
)


def make_run(
    pipeline: str = "etl",
    status: str = "success",
    started_at: str = "2024-01-01T10:00:00",
    duration: float | None = 30.0,
) -> PipelineRun:
    return PipelineRun(
        run_id=f"{pipeline}-{started_at}",
        pipeline=pipeline,
        status=PipelineStatus(status),
        started_at=started_at,
        finished_at=started_at,
        duration=duration,
    )


@pytest.fixture
def store_path(tmp_path: Path) -> Path:
    return tmp_path / "runs.jsonl"


def test_load_watermarks_missing_returns_empty(store_path: Path) -> None:
    assert load_watermarks(store_path) == []


def test_save_and_load_roundtrip(store_path: Path) -> None:
    entries = [
        WatermarkEntry("etl", "success_rate", 0.95, "2024-01-01T10:00:00"),
        WatermarkEntry("etl", "run_count", 20.0, "2024-01-01T10:00:00"),
    ]
    save_watermarks(store_path, entries)
    loaded = load_watermarks(store_path)
    assert len(loaded) == 2
    assert loaded[0].pipeline == "etl"
    assert loaded[0].metric == "success_rate"
    assert loaded[0].value == pytest.approx(0.95)


def test_compute_watermarks_success_rate(store_path: Path) -> None:
    runs = [
        make_run("etl", "success"),
        make_run("etl", "success"),
        make_run("etl", "failed"),
    ]
    marks = compute_watermarks(runs)
    sr = next(m for m in marks if m.metric == "success_rate")
    assert sr.value == pytest.approx(2 / 3)


def test_compute_watermarks_run_count(store_path: Path) -> None:
    runs = [make_run("etl") for _ in range(5)]
    marks = compute_watermarks(runs)
    rc = next(m for m in marks if m.metric == "run_count")
    assert rc.value == 5.0


def test_compute_watermarks_avg_duration(store_path: Path) -> None:
    runs = [
        make_run("etl", duration=10.0),
        make_run("etl", duration=30.0),
    ]
    marks = compute_watermarks(runs)
    ad = next(m for m in marks if m.metric == "avg_duration")
    assert ad.value == pytest.approx(20.0)


def test_compute_watermarks_no_duration_skips_metric() -> None:
    runs = [make_run("etl", duration=None)]
    marks = compute_watermarks(runs)
    metrics = {m.metric for m in marks}
    assert "avg_duration" not in metrics


def test_update_watermarks_keeps_best_value(store_path: Path) -> None:
    old = [WatermarkEntry("etl", "success_rate", 0.99, "2024-01-01T09:00:00")]
    save_watermarks(store_path, old)

    runs = [make_run("etl", "failed"), make_run("etl", "failed")]
    updated = update_watermarks(store_path, runs)

    sr = next(e for e in updated if e.metric == "success_rate" and e.pipeline == "etl")
    assert sr.value == pytest.approx(0.99)  # old mark preserved


def test_update_watermarks_replaces_when_improved(store_path: Path) -> None:
    old = [WatermarkEntry("etl", "success_rate", 0.5, "2024-01-01T09:00:00")]
    save_watermarks(store_path, old)

    runs = [make_run("etl", "success"), make_run("etl", "success")]
    updated = update_watermarks(store_path, runs)

    sr = next(e for e in updated if e.metric == "success_rate" and e.pipeline == "etl")
    assert sr.value == pytest.approx(1.0)


def test_watermark_entry_str() -> None:
    e = WatermarkEntry("my_pipe", "success_rate", 0.875, "2024-06-01T00:00:00")
    s = str(e)
    assert "my_pipe" in s
    assert "success_rate" in s
    assert "0.875" in s
