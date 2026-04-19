"""Tests for pipewatch.search."""
import pytest
from datetime import datetime
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.search import search_by_error, search_by_pipeline, search_runs


def make_run(name="pipe", status=PipelineStatus.SUCCESS, error=None, run_id=None):
    import uuid
    return PipelineRun(
        run_id=run_id or str(uuid.uuid4()),
        pipeline_name=name,
        status=status,
        started_at=datetime(2024, 1, 1, 12, 0, 0),
        finished_at=datetime(2024, 1, 1, 12, 1, 0),
        error=error,
    )


def test_search_by_error_match():
    runs = [make_run(error="NullPointerException in step 3"), make_run(error="timeout"), make_run()]
    result = search_by_error(runs, "timeout")
    assert len(result) == 1
    assert result[0].error == "timeout"


def test_search_by_error_case_insensitive():
    runs = [make_run(error="TimeoutError"), make_run(error="other")]
    result = search_by_error(runs, "timeout")
    assert len(result) == 1


def test_search_by_error_no_match():
    runs = [make_run(error="disk full"), make_run()]
    result = search_by_error(runs, "network")
    assert result == []


def test_search_by_error_none_errors_skipped():
    """Runs with no error message should not raise and should be excluded."""
    runs = [make_run(error=None), make_run(error=None), make_run(error="timeout")]
    result = search_by_error(runs, "timeout")
    assert len(result) == 1
    assert result[0].error == "timeout"


def test_search_by_pipeline_partial():
    runs = [make_run(name="etl_sales"), make_run(name="etl_inventory"), make_run(name="reporting")]
    result = search_by_pipeline(runs, "etl")
    assert len(result) == 2


def test_search_by_pipeline_case_insensitive():
    runs = [make_run(name="ETL_Sales"), make_run(name="reporting")]
    result = search_by_pipeline(runs, "etl")
    assert len(result) == 1


def test_search_runs_default_fields():
    runs = [
        make_run(name="etl_orders", error=None),
        make_run(name="reporting", error="etl step failed"),
        make_run(name="archive"),
    ]
    result = search_runs(runs, "etl")
    assert len(result) == 2


def test_search_runs_specific_field():
    runs = [make_run(name="etl_pipe", error="disk error"), make_run(name="other", error="etl error")]
    result = search_runs(runs, "etl", fields=["pipeline_name"])
    assert len(result) == 1
    assert result[0].pipeline_name == "etl_pipe"


def test_search_runs_empty():
    assert search_runs([], "anything") == []


def test_search_runs_no_match():
    runs = [make_run(name="pipe", error="timeout")]
    assert search_runs(runs, "zzznomatch") == []
