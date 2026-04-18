"""Tests for pipewatch.watcher tail_file and _parse_line."""

import json
import pytest
from pathlib import Path
from datetime import datetime, timezone

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore
from pipewatch.watcher import _parse_line, tail_file


def make_run_dict(**kwargs):
    defaults = {
        "run_id": "r1",
        "pipeline": "etl_main",
        "status": "success",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "ended_at": datetime.now(timezone.utc).isoformat(),
        "error": None,
    }
    defaults.update(kwargs)
    return defaults


@pytest.fixture
def tmp_store(tmp_path):
    return RunStore(tmp_path / "runs.jsonl")


def test_parse_line_valid():
    d = make_run_dict()
    run = _parse_line(json.dumps(d))
    assert isinstance(run, PipelineRun)
    assert run.pipeline == "etl_main"
    assert run.status == PipelineStatus.SUCCESS


def test_parse_line_empty():
    assert _parse_line("") is None
    assert _parse_line("   ") is None


def test_parse_line_invalid_json():
    assert _parse_line("{not valid json") is None


def test_parse_line_missing_field():
    d = make_run_dict()
    del d["pipeline"]
    assert _parse_line(json.dumps(d)) is None


def test_parse_line_bad_status():
    d = make_run_dict(status="unknown_status")
    assert _parse_line(json.dumps(d)) is None


def test_tail_file_ingests_runs(tmp_path, tmp_store):
    log_file = tmp_path / "pipeline.ndjson"
    runs = [
        make_run_dict(run_id="r1", pipeline="etl_a", status="success"),
        make_run_dict(run_id="r2", pipeline="etl_b", status="failed", error="timeout"),
    ]
    with open(log_file, "w") as fh:
        for r in runs:
            fh.write(json.dumps(r) + "\n")

    collected = []
    tail_file(log_file, tmp_store, on_run=collected.append, poll_interval=0, max_iterations=1)

    assert len(collected) == 2
    assert tmp_store.load_all()[0].run_id == "r1"
    assert tmp_store.load_all()[1].status == PipelineStatus.FAILED


def test_tail_file_missing_file_does_not_crash(tmp_path, tmp_store):
    missing = tmp_path / "nonexistent.ndjson"
    # Should complete without error
    tail_file(missing, tmp_store, poll_interval=0, max_iterations=1)
    assert tmp_store.load_all() == []
