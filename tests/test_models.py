import pytest
from datetime import datetime
from pipewatch.models import PipelineRun, PipelineStatus


def make_run(**kwargs):
    defaults = dict(
        pipeline_id="etl_orders",
        run_id="run_001",
        status=PipelineStatus.SUCCESS,
        started_at=datetime(2024, 1, 10, 8, 0, 0),
        finished_at=datetime(2024, 1, 10, 8, 5, 30),
    )
    defaults.update(kwargs)
    return PipelineRun(**defaults)


def test_duration_calculated_on_init():
    run = make_run()
    assert run.duration_seconds == 330.0


def test_is_failed():
    run = make_run(status=PipelineStatus.FAILURE)
    assert run.is_failed is True
    assert run.is_success is False


def test_is_success():
    run = make_run(status=PipelineStatus.SUCCESS)
    assert run.is_success is True
    assert run.is_failed is False


def test_to_dict_roundtrip():
    run = make_run(error_message="timeout", tags={"env": "prod"})
    data = run.to_dict()
    restored = PipelineRun.from_dict(data)
    assert restored.pipeline_id == run.pipeline_id
    assert restored.status == run.status
    assert restored.tags == {"env": "prod"}
    assert restored.duration_seconds == run.duration_seconds


def test_from_dict_no_finished_at():
    run = make_run(finished_at=None)
    data = run.to_dict()
    restored = PipelineRun.from_dict(data)
    assert restored.finished_at is None
