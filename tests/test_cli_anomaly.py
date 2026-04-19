import argparse
import pytest
from pathlib import Path
from datetime import datetime, timezone
from pipewatch.store import RunStore
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.cli_anomaly import cmd_anomaly
import uuid


def make_run(pipeline: str, duration: float, status: PipelineStatus = PipelineStatus.SUCCESS) -> PipelineRun:
    now = datetime.now(timezone.utc).isoformat()
    return PipelineRun(
        run_id=str(uuid.uuid4()),
        pipeline=pipeline,
        status=status,
        started_at=now,
        ended_at=now,
        duration=duration,
    )


@pytest.fixture
def store_path(tmp_path):
    return tmp_path / "runs.jsonl"


def make_args(store_path, pipeline=None, z_threshold=2.5):
    return argparse.Namespace(store=str(store_path), pipeline=pipeline, z_threshold=z_threshold)


def seed(store_path, runs):
    s = RunStore(str(store_path))
    for r in runs:
        s.append(r)


def test_anomaly_empty_store(store_path, capsys):
    cmd_anomaly(make_args(store_path))
    out = capsys.readouterr().out
    assert "No runs" in out


def test_anomaly_no_anomalies(store_path, capsys):
    runs = [make_run("etl", 10.0) for _ in range(6)]
    seed(store_path, runs)
    cmd_anomaly(make_args(store_path))
    out = capsys.readouterr().out
    assert "No anomalies" in out


def test_anomaly_detects_outlier(store_path, capsys):
    runs = [make_run("etl", 10.0) for _ in range(8)]
    runs.append(make_run("etl", 9999.0))
    seed(store_path, runs)
    cmd_anomaly(make_args(store_path))
    out = capsys.readouterr().out
    assert "anomaly" in out.lower()


def test_anomaly_filter_pipeline(store_path, capsys):
    runs_a = [make_run("pipe-a", 10.0) for _ in range(8)]
    runs_a.append(make_run("pipe-a", 9999.0))
    runs_b = [make_run("pipe-b", 10.0) for _ in range(8)]
    seed(store_path, runs_a + runs_b)
    cmd_anomaly(make_args(store_path, pipeline="pipe-b"))
    out = capsys.readouterr().out
    assert "No anomalies" in out
