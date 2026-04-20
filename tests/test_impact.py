"""Tests for pipewatch.impact."""
import pytest
from pipewatch.pipeline_map import PipelineMap
from pipewatch.impact import compute_impact, impact_summary, ImpactResult


def make_linear_map() -> PipelineMap:
    """ingest -> transform -> load"""
    pm = PipelineMap()
    pm.add_edge("ingest", "transform")
    pm.add_edge("transform", "load")
    return pm


def test_no_impact_for_leaf():
    pm = make_linear_map()
    result = compute_impact(pm, "load")
    assert result.affected == []
    assert result.depth == 0


def test_direct_downstream():
    pm = make_linear_map()
    result = compute_impact(pm, "transform")
    assert "load" in result.affected
    assert "transform" not in result.affected


def test_transitive_downstream():
    pm = make_linear_map()
    result = compute_impact(pm, "ingest")
    assert set(result.affected) == {"transform", "load"}


def test_depth_linear():
    pm = make_linear_map()
    result = compute_impact(pm, "ingest")
    assert result.depth == 2


def test_branching_downstream():
    pm = PipelineMap()
    pm.add_edge("root", "branch_a")
    pm.add_edge("root", "branch_b")
    result = compute_impact(pm, "root")
    assert set(result.affected) == {"branch_a", "branch_b"}


def test_no_cycles_on_diamond():
    pm = PipelineMap()
    pm.add_edge("a", "b")
    pm.add_edge("a", "c")
    pm.add_edge("b", "d")
    pm.add_edge("c", "d")
    result = compute_impact(pm, "a")
    assert set(result.affected) == {"b", "c", "d"}
    assert result.affected.count("d") == 1


def test_impact_summary_multiple():
    pm = make_linear_map()
    results = impact_summary(pm, ["ingest", "transform"])
    assert len(results) == 2
    roots = [r.root for r in results]
    assert "ingest" in roots and "transform" in roots


def test_str_no_impact():
    pm = PipelineMap()
    pm.add_edge("a", "b")
    result = compute_impact(pm, "b")
    assert "No downstream" in str(result)


def test_str_with_impact():
    pm = make_linear_map()
    result = compute_impact(pm, "ingest")
    out = str(result)
    assert "ingest" in out
    assert "transform" in out
    assert "load" in out
