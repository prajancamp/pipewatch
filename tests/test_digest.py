"""Tests for pipewatch.digest."""

from datetime import datetime, timedelta
from pathlib import Path
import pytest

from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore
from pipewatch.digest import build_digest, DigestReport


def make_run(name, status, minutes_ago=10):
    ts = (datetime.utcnow() - timedelta(minutes=minutes_ago)).isoformat()
    return PipelineRun(
        pipeline_name=name,
        run_id=f"{name}-{minutes_ago}",
        status=status,
        started_at=ts,
        ended_at=ts,
    )


@pytest.fixture
def tmp_store(tmp_path):
    return RunStore(tmp_path / "runs.jsonl")


def test_digest_empty_store(tmp_store):
    digest = build_digest(tmp_store, period_hours=24)
    assert digest.total_runs == 0
    assert digest.pipeline_count == 0
    assert digest.failed_pipelines == []


def test_digest_counts_recent_runs(tmp_store):
    tmp_store.append(make_run("etl_a", PipelineStatus.SUCCESS, minutes_ago=30))
    tmp_store.append(make_run("etl_a", PipelineStatus.FAILED, minutes_ago=20))
    tmp_store.append(make_run("etl_b", PipelineStatus.SUCCESS, minutes_ago=10))

    digest = build_digest(tmp_store, period_hours=24)
    assert digest.total_runs == 3
    assert digest.pipeline_count == 2


def test_digest_excludes_old_runs(tmp_store):
    tmp_store.append(make_run("etl_a", PipelineStatus.SUCCESS, minutes_ago=2000))
    tmp_store.append(make_run("etl_b", PipelineStatus.SUCCESS, minutes_ago=10))

    digest = build_digest(tmp_store, period_hours=24)
    assert digest.total_runs == 1
    assert digest.pipeline_count == 1


def test_digest_identifies_failing_pipelines(tmp_store):
    tmp_store.append(make_run("good", PipelineStatus.SUCCESS, minutes_ago=5))
    tmp_store.append(make_run("bad", PipelineStatus.FAILED, minutes_ago=5))
    tmp_store.append(make_run("bad", PipelineStatus.FAILED, minutes_ago=15))

    digest = build_digest(tmp_store, period_hours=24)
    assert "bad" in digest.failed_pipelines
    assert "good" not in digest.failed_pipelines


def test_digest_str_contains_header(tmp_store):
    tmp_store.append(make_run("etl_a", PipelineStatus.SUCCESS, minutes_ago=5))
    digest = build_digest(tmp_store, period_hours=12)
    text = str(digest)
    assert "PipeWatch Digest" in text
    assert "12h" in text
