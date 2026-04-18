"""Tests for the pipewatch CLI."""
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.cli import build_parser, cmd_report, cmd_ingest


@pytest.fixture
def store_path(tmp_path) -> Path:
    return tmp_path / "runs.jsonl"


def make_args(**kwargs):
    """Build a simple namespace for CLI args."""
    import argparse
    defaults = {"store": "runs.jsonl", "pipeline": None, "consecutive_threshold": 3}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_ingest_creates_record(store_path):
    import argparse
    args = argparse.Namespace(
        pipeline="etl_daily",
        status="success",
        started=None,
        ended=None,
        store=str(store_path),
        error=None,
    )
    result = cmd_ingest(args)
    assert result == 0
    assert store_path.exists()
    lines = store_path.read_text().strip().splitlines()
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert data["pipeline_name"] == "etl_daily"
    assert data["status"] == "success"


def test_ingest_failed_with_error(store_path):
    import argparse
    args = argparse.Namespace(
        pipeline="etl_hourly",
        status="failed",
        started=None,
        ended=None,
        store=str(store_path),
        error="Connection timeout",
    )
    cmd_ingest(args)
    data = json.loads(store_path.read_text().strip())
    assert data["error_message"] == "Connection timeout"


def test_report_empty_store(store_path, capsys):
    args = make_args(store=str(store_path))
    result = cmd_report(args)
    assert result == 0


def test_report_with_data(store_path, capsys):
    import argparse
    for status in ["success", "failed", "failed"]:
        ingest_args = argparse.Namespace(
            pipeline="pipe_a", status=status, started=None,
            ended=None, store=str(store_path), error=None,
        )
        cmd_ingest(ingest_args)

    report_args = make_args(store=str(store_path))
    result = cmd_report(report_args)
    assert result == 0
    captured = capsys.readouterr()
    assert "pipe_a" in captured.out


def test_parser_report_defaults():
    parser = build_parser()
    args = parser.parse_args(["report"])
    assert args.store == "runs.jsonl"
    assert args.consecutive_threshold == 3


def test_parser_ingest_required_fields():
    parser = build_parser()
    args = parser.parse_args(["ingest", "my_pipe", "failed"])
    assert args.pipeline == "my_pipe"
    assert args.status == "failed"
