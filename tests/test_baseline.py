"""Tests for pipewatch.baseline."""
import pytest
from pathlib import Path
from pipewatch.baseline import (
    BaselineEntry,
    BaselineDiff,
    save_baseline,
    load_baseline,
    diff_baseline,
)


def make_entry(pipeline="etl", avg_duration=10.0, success_rate=0.9, sample_size=20):
    return BaselineEntry(
        pipeline=pipeline,
        avg_duration=avg_duration,
        success_rate=success_rate,
        sample_size=sample_size,
    )


def test_entry_roundtrip():
    e = make_entry()
    assert BaselineEntry.from_dict(e.to_dict()) == e


def test_entry_roundtrip_no_duration():
    e = make_entry(avg_duration=None)
    assert BaselineEntry.from_dict(e.to_dict()) == e


def test_save_and_load(tmp_path):
    p = tmp_path / "baseline.json"
    entries = [make_entry("a"), make_entry("b", success_rate=0.75)]
    save_baseline(entries, path=p)
    loaded = load_baseline(path=p)
    assert len(loaded) == 2
    assert loaded[0].pipeline == "a"
    assert loaded[1].success_rate == pytest.approx(0.75)


def test_load_missing_returns_empty(tmp_path):
    result = load_baseline(path=tmp_path / "nonexistent.json")
    assert result == []


def test_diff_baseline_detects_regression():
    current = [make_entry("etl", avg_duration=15.0, success_rate=0.7)]
    baseline = [make_entry("etl", avg_duration=10.0, success_rate=0.9)]
    diffs = diff_baseline(current, baseline)
    assert len(diffs) == 1
    d = diffs[0]
    assert d.pipeline == "etl"
    assert d.success_rate_delta == pytest.approx(-0.2)
    assert d.duration_delta == pytest.approx(5.0)


def test_diff_baseline_skips_new_pipelines():
    current = [make_entry("new_pipeline")]
    baseline = [make_entry("etl")]
    diffs = diff_baseline(current, baseline)
    assert diffs == []


def test_diff_str_output():
    d = BaselineDiff(pipeline="etl", success_rate_delta=-0.1, duration_delta=3.5)
    text = str(d)
    assert "etl" in text
    assert "-10.0%" in text
    assert "+3.5s" in text or "3.5s" in text


def test_diff_no_duration_delta():
    d = BaselineDiff(pipeline="etl", success_rate_delta=0.05, duration_delta=None)
    text = str(d)
    assert "avg_duration" not in text
