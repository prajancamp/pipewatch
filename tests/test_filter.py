"""Tests for pipewatch.filter."""
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.filter import filter_runs, unique_pipelines, latest_run_per_pipeline


def make_run(
    pipeline: str = "etl",
    status: PipelineStatus = PipelineStatus.SUCCESS,
    started_at: datetime = None,
) -> PipelineRun:
    started_at = started_at or datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    return PipelineRun(
        run_id=str(uuid4()),
        pipeline_name=pipeline,
        status=status,
        started_at=started_at,
        ended_at=started_at,
    )


def test_filter_by_pipeline():
    runs = [make_run("a"), make_run("b"), make_run("a")]
    assert [r.pipeline_name for r in filter_runs(runs, pipeline="a")] == ["a", "a"]


def test_filter_by_status():
    runs = [make_run(status=PipelineStatus.SUCCESS), make_run(status=PipelineStatus.FAILED)]
    result = filter_runs(runs, status=PipelineStatus.FAILED)
    assert len(result) == 1
    assert result[0].status == PipelineStatus.FAILED


def test_filter_since():
    early = datetime(2024, 1, 1, tzinfo=timezone.utc)
    late = datetime(2024, 6, 1, tzinfo=timezone.utc)
    runs = [make_run(started_at=early), make_run(started_at=late)]
    result = filter_runs(runs, since=datetime(2024, 3, 1, tzinfo=timezone.utc))
    assert len(result) == 1
    assert result[0].started_at == late


def test_filter_until():
    early = datetime(2024, 1, 1, tzinfo=timezone.utc)
    late = datetime(2024, 6, 1, tzinfo=timezone.utc)
    runs = [make_run(started_at=early), make_run(started_at=late)]
    result = filter_runs(runs, until=datetime(2024, 3, 1, tzinfo=timezone.utc))
    assert len(result) == 1
    assert result[0].started_at == early


def test_filter_no_criteria_returns_all():
    runs = [make_run(), make_run()]
    assert filter_runs(runs) == runs


def test_unique_pipelines():
    runs = [make_run("b"), make_run("a"), make_run("b")]
    assert unique_pipelines(runs) == ["a", "b"]


def test_latest_run_per_pipeline():
    old = make_run("etl", started_at=datetime(2024, 1, 1, tzinfo=timezone.utc))
    new = make_run("etl", started_at=datetime(2024, 6, 1, tzinfo=timezone.utc))
    other = make_run("loader")
    result = latest_run_per_pipeline([old, new, other])
    assert len(result) == 2
    etl_result = next(r for r in result if r.pipeline_name == "etl")
    assert etl_result.started_at == new.started_at
