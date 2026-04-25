"""Tests for pipewatch.topology."""
from __future__ import annotations

import pytest

from pipewatch.pipeline_map import PipelineMap
from pipewatch.topology import TopologyResult, analyze_topology, _count_reachable


def _make_map(*edges: tuple) -> PipelineMap:
    """Build a PipelineMap from (upstream, downstream) pairs."""
    pm = PipelineMap()
    for up, down in edges:
        pm.add_edge(up, down)
    return pm


# ---------------------------------------------------------------------------
# _count_reachable
# ---------------------------------------------------------------------------

def test_count_reachable_empty_adjacency():
    assert _count_reachable("a", {}) == 0


def test_count_reachable_direct_neighbours():
    adj = {"a": ["b", "c"], "b": [], "c": []}
    assert _count_reachable("a", adj) == 2


def test_count_reachable_transitive():
    adj = {"a": ["b"], "b": ["c"], "c": []}
    assert _count_reachable("a", adj) == 2  # b and c


def test_count_reachable_no_cycle_double_count():
    adj = {"a": ["b", "c"], "b": ["c"], "c": []}
    assert _count_reachable("a", adj) == 2  # b and c, c not double-counted


# ---------------------------------------------------------------------------
# analyze_topology
# ---------------------------------------------------------------------------

def test_analyze_topology_empty_map():
    pm = PipelineMap()
    assert analyze_topology(pm) == []


def test_analyze_topology_linear_chain():
    # a -> b -> c
    pm = _make_map(("a", "b"), ("b", "c"))
    results = analyze_topology(pm)
    by_name = {r.pipeline: r for r in results}

    # 'a' reaches b and c downstream; no upstream
    assert by_name["a"].downstream_count == 2
    assert by_name["a"].upstream_count == 0

    # 'c' has no downstream; reaches a and b upstream
    assert by_name["c"].downstream_count == 0
    assert by_name["c"].upstream_count == 2


def test_analyze_topology_sorted_by_influence():
    # a -> b, a -> c, a -> d  (a has highest influence)
    pm = _make_map(("a", "b"), ("a", "c"), ("a", "d"))
    results = analyze_topology(pm)
    assert results[0].pipeline == "a"


def test_analyze_topology_hub_flag():
    # a -> b, a -> c, a -> d  (3 downstream == hub_threshold default 3)
    pm = _make_map(("a", "b"), ("a", "c"), ("a", "d"))
    results = analyze_topology(pm)
    by_name = {r.pipeline: r for r in results}
    assert by_name["a"].is_hub is True
    assert by_name["b"].is_hub is False


def test_analyze_topology_custom_hub_threshold():
    pm = _make_map(("a", "b"), ("a", "c"))
    results = analyze_topology(pm, hub_threshold=2)
    by_name = {r.pipeline: r for r in results}
    assert by_name["a"].is_hub is True


def test_topology_result_str_includes_hub_tag():
    r = TopologyResult(pipeline="etl", upstream_count=1, downstream_count=4,
                       influence_score=4.5, is_hub=True)
    assert "[HUB]" in str(r)


def test_topology_result_str_no_hub_tag():
    r = TopologyResult(pipeline="etl", upstream_count=1, downstream_count=1,
                       influence_score=1.5, is_hub=False)
    assert "[HUB]" not in str(r)
