"""Tests for pipewatch.tag module."""
import pytest
from datetime import datetime
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.tag import (
    runs_with_tag, runs_without_tag, group_by_tag,
    all_tags, filter_by_tags,
)


def make_run(pipeline: str = "pipe", status: PipelineStatus = PipelineStatus.SUCCESS, tags=None) -> PipelineRun:
    return PipelineRun(
        pipeline_name=pipeline,
        status=status,
        started_at=datetime(2024, 1, 1, 12, 0, 0),
        ended_at=datetime(2024, 1, 1, 12, 1, 0),
        tags=tags or [],
    )


def test_runs_with_tag():
    runs = [make_run(tags=["prod"]), make_run(tags=["dev"]), make_run(tags=["prod", "critical"])]
    result = runs_with_tag(runs, "prod")
    assert len(result) == 2


def test_runs_without_tag():
    runs = [make_run(tags=["prod"]), make_run(tags=["dev"]), make_run(tags=[])]
    result = runs_without_tag(runs, "prod")
    assert len(result) == 2


def test_group_by_tag():
    runs = [make_run(tags=["prod"]), make_run(tags=["dev"]), make_run(tags=["prod", "dev"])]
    groups = group_by_tag(runs)
    assert len(groups["prod"]) == 2
    assert len(groups["dev"]) == 2


def test_group_by_tag_empty():
    runs = [make_run(tags=[])]
    groups = group_by_tag(runs)
    assert groups == {}


def test_all_tags():
    runs = [make_run(tags=["prod", "critical"]), make_run(tags=["dev"]), make_run(tags=[])]
    tags = all_tags(runs)
    assert tags == ["critical", "dev", "prod"]


def test_filter_by_tags_any():
    runs = [make_run(tags=["prod"]), make_run(tags=["dev"]), make_run(tags=["prod", "dev"])]
    result = filter_by_tags(runs, ["prod", "critical"], match_all=False)
    assert len(result) == 2


def test_filter_by_tags_all():
    runs = [make_run(tags=["prod"]), make_run(tags=["dev"]), make_run(tags=["prod", "dev"])]
    result = filter_by_tags(runs, ["prod", "dev"], match_all=True)
    assert len(result) == 1


def test_filter_by_tags_empty_tags_returns_all():
    runs = [make_run(tags=["prod"]), make_run(tags=["dev"])]
    result = filter_by_tags(runs, [])
    assert result == runs
