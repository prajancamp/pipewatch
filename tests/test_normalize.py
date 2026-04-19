"""Tests for pipewatch.normalize."""

import pytest
from datetime import datetime, timezone

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.normalize import (
    normalize_pipeline_name,
    normalize_error,
    normalize_tags,
    normalize_run,
    normalize_runs,
)


def make_run(**kwargs) -> PipelineRun:
    defaults = dict(
        run_id="r1",
        pipeline="My Pipeline",
        status=PipelineStatus.SUCCESS,
        started_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
        ended_at=datetime(2024, 1, 1, 10, 5, tzinfo=timezone.utc),
        error=None,
        tags=[],
        meta={},
    )
    defaults.update(kwargs)
    return PipelineRun(**defaults)


def test_normalize_pipeline_name_basic():
    assert normalize_pipeline_name("My Pipeline") == "my_pipeline"


def test_normalize_pipeline_name_dashes():
    assert normalize_pipeline_name("ETL-Job-Daily") == "etl_job_daily"


def test_normalize_pipeline_name_strips_special():
    assert normalize_pipeline_name("  pipe#1! ") == "pipe1"


def test_normalize_error_none():
    assert normalize_error(None) is None


def test_normalize_error_blank():
    assert normalize_error("   ") is None


def test_normalize_error_truncates():
    long_err = "x" * 600
    result = normalize_error(long_err)
    assert len(result) == 500


def test_normalize_error_strips():
    assert normalize_error("  some error  ") == "some error"


def test_normalize_tags_deduplicates():
    assert normalize_tags(["A", "a", "B"]) == ["a", "b"]


def test_normalize_tags_sorts():
    assert normalize_tags(["zebra", "apple"]) == ["apple", "zebra"]


def test_normalize_tags_none_returns_empty():
    assert normalize_tags(None) == []


def test_normalize_tags_filters_blank():
    assert normalize_tags(["", "  ", "ok"]) == ["ok"]


def test_normalize_run_updates_fields():
    run = make_run(pipeline="My-Pipeline", error="  bad thing  ", tags=["X", "x"])
    result = normalize_run(run)
    assert result.pipeline == "my_pipeline"
    assert result.error == "bad thing"
    assert result.tags == ["x"]


def test_normalize_run_preserves_other_fields():
    run = make_run(run_id="abc123")
    result = normalize_run(run)
    assert result.run_id == "abc123"
    assert result.status == PipelineStatus.SUCCESS


def test_normalize_runs_applies_to_all():
    runs = [make_run(pipeline="A B"), make_run(pipeline="C-D")]
    results = normalize_runs(runs)
    assert results[0].pipeline == "a_b"
    assert results[1].pipeline == "c_d"
