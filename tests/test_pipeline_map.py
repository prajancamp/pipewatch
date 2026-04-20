"""Tests for pipewatch.pipeline_map."""
import json
import pytest
from pathlib import Path
from pipewatch.pipeline_map import PipelineMap, PipelineNode, load_map, save_map


@pytest.fixture
def store_path(tmp_path):
    return str(tmp_path / "runs.jsonl")


def test_add_edge_creates_nodes():
    pm = PipelineMap()
    pm.add_edge("ingest", "transform")
    assert "ingest" in pm.nodes
    assert "transform" in pm.nodes


def test_add_edge_upstream_downstream():
    pm = PipelineMap()
    pm.add_edge("ingest", "transform")
    assert "transform" in pm.get_downstream("ingest")
    assert "ingest" in pm.get_upstream("transform")


def test_add_edge_idempotent():
    pm = PipelineMap()
    pm.add_edge("a", "b")
    pm.add_edge("a", "b")
    assert pm.nodes["a"].downstream.count("b") == 1


def test_all_pipelines_sorted():
    pm = PipelineMap()
    pm.add_edge("z_pipe", "a_pipe")
    assert pm.all_pipelines() == ["a_pipe", "z_pipe"]


def test_get_upstream_unknown_pipeline():
    pm = PipelineMap()
    assert pm.get_upstream("ghost") == []


def test_get_downstream_unknown_pipeline():
    pm = PipelineMap()
    assert pm.get_downstream("ghost") == []


def test_roundtrip_to_from_dict():
    pm = PipelineMap()
    pm.add_edge("a", "b")
    pm.add_edge("b", "c")
    pm2 = PipelineMap.from_dict(pm.to_dict())
    assert pm2.get_downstream("a") == ["b"]
    assert pm2.get_upstream("c") == ["b"]


def test_save_and_load(store_path):
    pm = PipelineMap()
    pm.add_edge("src", "dst")
    save_map(store_path, pm)
    pm2 = load_map(store_path)
    assert pm2.get_downstream("src") == ["dst"]


def test_load_missing_returns_empty(store_path):
    pm = load_map(store_path)
    assert pm.nodes == {}
