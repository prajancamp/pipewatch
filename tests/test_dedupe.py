"""Tests for pipewatch.dedupe."""
import pytest
from datetime import datetime, timezone
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.dedupe import dedupe_by_run_id, dedupe_latest_per_pipeline, find_duplicate_run_ids


def make_run(run_id: str, pipeline: str = "etl", started_at: datetime = None, status=PipelineStatus.SUCCESS):
    return PipelineRun(
        run_id=run_id,
        pipeline_name=pipeline,
        status=status,
        started_at=started_at or datetime(2024, 1, 1, tzinfo=timezone.utc),
        ended_at=datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
    )


def test_dedupe_by_run_id_no_duplicates():
    runs = [make_run("a"), make_run("b"), make_run("c")]
    result = dedupe_by_run_id(runs)
    assert len(result) == 3


def test_dedupe_by_run_id_removes_duplicates():
    runs = [make_run("a"), make_run("a"), make_run("b")]
    result = dedupe_by_run_id(runs)
    assert len(result) == 2
    assert result[0].run_id == "a"
    assert result[1].run_id == "b"


def test_dedupe_by_run_id_first_wins():
    r1 = make_run("dup", pipeline="first")
    r2 = make_run("dup", pipeline="second")
    result = dedupe_by_run_id([r1, r2])
    assert len(result) == 1
    assert result[0].pipeline_name == "first"


def test_dedupe_latest_per_pipeline():
    early = make_run("r1", pipeline="etl", started_at=datetime(2024, 1, 1, tzinfo=timezone.utc))
    late = make_run("r2", pipeline="etl", started_at=datetime(2024, 1, 2, tzinfo=timezone.utc))
    result = dedupe_latest_per_pipeline([early, late])
    assert len(result) == 1
    assert result[0].run_id == "r2"


def test_dedupe_latest_per_pipeline_multiple_pipelines():
    runs = [
        make_run("a1", pipeline="alpha", started_at=datetime(2024, 1, 1, tzinfo=timezone.utc)),
        make_run("b1", pipeline="beta", started_at=datetime(2024, 1, 3, tzinfo=timezone.utc)),
        make_run("a2", pipeline="alpha", started_at=datetime(2024, 1, 5, tzinfo=timezone.utc)),
    ]
    result = dedupe_latest_per_pipeline(runs)
    by_name = {r.pipeline_name: r for r in result}
    assert by_name["alpha"].run_id == "a2"
    assert by_name["beta"].run_id == "b1"


def test_find_duplicate_run_ids():
    runs = [make_run("x"), make_run("x"), make_run("y"), make_run("y"), make_run("z")]
    dupes = find_duplicate_run_ids(runs)
    assert set(dupes) == {"x", "y"}


def test_find_duplicate_run_ids_none():
    runs = [make_run("a"), make_run("b")]
    assert find_duplicate_run_ids(runs) == []
