"""Tests for pipewatch.export."""

import json
import csv
import io
import pytest
from datetime import datetime, timezone

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.export import export_runs_json, export_runs_csv, write_export


def make_run(pipeline="etl", status=PipelineStatus.SUCCESS, tags=None, error=None):
    return PipelineRun(
        run_id=f"r-{pipeline}-{status.value}",
        pipeline=pipeline,
        status=status,
        started_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        ended_at=datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
        error=error,
        tags=tags or [],
    )


def test_export_runs_json_all():
    runs = [make_run("a"), make_run("b", PipelineStatus.FAILED)]
    result = json.loads(export_runs_json(runs))
    assert len(result) == 2


def test_export_runs_json_filter_pipeline():
    runs = [make_run("a"), make_run("b")]
    result = json.loads(export_runs_json(runs, pipeline="a"))
    assert len(result) == 1
    assert result[0]["pipeline"] == "a"


def test_export_runs_json_filter_status():
    runs = [make_run("a"), make_run("b", PipelineStatus.FAILED)]
    result = json.loads(export_runs_json(runs, status="failed"))
    assert len(result) == 1
    assert result[0]["status"] == "failed"


def test_export_runs_csv_headers():
    runs = [make_run("etl", tags=["prod"])]
    output = export_runs_csv(runs)
    reader = csv.DictReader(io.StringIO(output))
    rows = list(reader)
    assert len(rows) == 1
    assert rows[0]["pipeline"] == "etl"
    assert rows[0]["tags"] == "prod"


def test_export_runs_csv_empty():
    result = export_runs_csv([])
    assert result == ""


def test_export_runs_csv_multiple_tags():
    runs = [make_run("etl", tags=["prod", "nightly"])]
    output = export_runs_csv(runs)
    reader = csv.DictReader(io.StringIO(output))
    rows = list(reader)
    assert rows[0]["tags"] == "prod|nightly"


def test_write_export_json(tmp_path):
    runs = [make_run("etl"), make_run("etl", PipelineStatus.FAILED)]
    out = tmp_path / "out.json"
    count = write_export(runs, str(out), fmt="json")
    assert count == 2
    data = json.loads(out.read_text())
    assert len(data) == 2


def test_write_export_csv(tmp_path):
    runs = [make_run("etl")]
    out = tmp_path / "out.csv"
    count = write_export(runs, str(out), fmt="csv")
    assert count == 1
    assert "etl" in out.read_text()


def test_write_export_invalid_format(tmp_path):
    with pytest.raises(ValueError, match="Unsupported format"):
        write_export([], str(tmp_path / "x"), fmt="xml")
