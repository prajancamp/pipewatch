"""Tests for pipewatch.summary export utilities."""
import csv
import io
import json
from datetime import datetime, timezone

import pytest

from pipewatch.analyzer import PipelineStats
from pipewatch.models import PipelineStatus
from pipewatch.summary import export_json, export_csv, export_summary, stats_to_dict


def make_stats(pipeline="etl", total=10, success=8, failure=2,
               avg_duration=42.5, last_status=PipelineStatus.SUCCESS):
    return PipelineStats(
        pipeline=pipeline,
        total_runs=total,
        success_count=success,
        failure_count=failure,
        success_rate=success / total if total else 0.0,
        avg_duration=avg_duration,
        last_status=last_status,
    )


def test_stats_to_dict_fields():
    s = make_stats()
    d = stats_to_dict(s)
    assert d["pipeline"] == "etl"
    assert d["total_runs"] == 10
    assert d["success_count"] == 8
    assert d["failure_count"] == 2
    assert d["success_rate"] == 0.8
    assert d["avg_duration"] == 42.5
    assert d["last_status"] == "success"


def test_stats_to_dict_none_duration():
    s = make_stats(avg_duration=None)
    d = stats_to_dict(s)
    assert d["avg_duration"] is None


def test_export_json_valid():
    stats_list = [make_stats("pipe_a"), make_stats("pipe_b", total=5, success=3, failure=2)]
    result = export_json(stats_list)
    parsed = json.loads(result)
    assert len(parsed) == 2
    assert parsed[0]["pipeline"] == "pipe_a"
    assert parsed[1]["pipeline"] == "pipe_b"


def test_export_csv_valid():
    stats_list = [make_stats("pipe_x")]
    result = export_csv(stats_list)
    reader = csv.DictReader(io.StringIO(result))
    rows = list(reader)
    assert len(rows) == 1
    assert rows[0]["pipeline"] == "pipe_x"
    assert rows[0]["success_count"] == "8"


def test_export_summary_json():
    result = export_summary([make_stats()], fmt="json")
    assert json.loads(result)[0]["pipeline"] == "etl"


def test_export_summary_csv():
    result = export_summary([make_stats()], fmt="csv")
    assert "etl" in result
    assert "pipeline" in result


def test_export_summary_invalid_format():
    with pytest.raises(ValueError, match="Unsupported format"):
        export_summary([make_stats()], fmt="xml")
