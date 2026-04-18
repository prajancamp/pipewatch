import pytest
from datetime import datetime
from pathlib import Path
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore


@pytest.fixture
def tmp_store(tmp_path):
    return RunStore(store_path=tmp_path / "test_runs.jsonl")


def make_run(pipeline_id="pipe_a", run_id="r1", status=PipelineStatus.SUCCESS):
    return PipelineRun(
        pipeline_id=pipeline_id,
        run_id=run_id,
        status=status,
        started_at=datetime(2024, 3, 1, 10, 0, 0),
        finished_at=datetime(2024, 3, 1, 10, 1, 0),
    )


def test_append_and_load(tmp_store):
    run = make_run()
    tmp_store.append(run)
    runs = tmp_store.load_all()
    assert len(runs) == 1
    assert runs[0].run_id == "r1"


def test_load_empty_store(tmp_store):
    assert tmp_store.load_all() == []


def test_load_by_pipeline(tmp_store):
    tmp_store.append(make_run(pipeline_id="pipe_a", run_id="r1"))
    tmp_store.append(make_run(pipeline_id="pipe_b", run_id="r2"))
    results = tmp_store.load_by_pipeline("pipe_a")
    assert len(results) == 1
    assert results[0].pipeline_id == "pipe_a"


def test_load_failures(tmp_store):
    tmp_store.append(make_run(run_id="ok", status=PipelineStatus.SUCCESS))
    tmp_store.append(make_run(run_id="fail", status=PipelineStatus.FAILURE))
    failures = tmp_store.load_failures()
    assert len(failures) == 1
    assert failures[0].run_id == "fail"


def test_clear(tmp_store):
    tmp_store.append(make_run())
    tmp_store.clear()
    assert tmp_store.load_all() == []
