"""Topology analysis: rank pipelines by influence (fan-out + failure propagation risk)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from pipewatch.pipeline_map import PipelineMap


@dataclass
class TopologyResult:
    pipeline: str
    upstream_count: int
    downstream_count: int
    influence_score: float  # weighted: downstream + 0.5 * upstream
    is_hub: bool  # True when downstream_count >= hub_threshold

    def __str__(self) -> str:
        hub_tag = " [HUB]" if self.is_hub else ""
        return (
            f"{self.pipeline}{hub_tag}  "
            f"upstream={self.upstream_count}  "
            f"downstream={self.downstream_count}  "
            f"influence={self.influence_score:.1f}"
        )


def _count_reachable(start: str, adjacency: Dict[str, List[str]]) -> int:
    """BFS count of all reachable nodes from *start* via *adjacency*."""
    visited: set = set()
    queue = [start]
    while queue:
        node = queue.pop()
        for neighbour in adjacency.get(node, []):
            if neighbour not in visited:
                visited.add(neighbour)
                queue.append(neighbour)
    return len(visited)


def analyze_topology(
    pipeline_map: PipelineMap,
    hub_threshold: int = 3,
) -> List[TopologyResult]:
    """Return a TopologyResult for every pipeline in *pipeline_map*,
    sorted descending by influence_score."""
    results: List[TopologyResult] = []

    for name in pipeline_map.all_pipelines():
        node = pipeline_map.nodes.get(name)
        if node is None:
            continue

        # Transitive counts via BFS
        upstream_count = _count_reachable(
            name,
            {n: list(pipeline_map.nodes[n].upstream) for n in pipeline_map.nodes},
        )
        downstream_count = _count_reachable(
            name,
            {n: list(pipeline_map.nodes[n].downstream) for n in pipeline_map.nodes},
        )

        influence = downstream_count + 0.5 * upstream_count
        results.append(
            TopologyResult(
                pipeline=name,
                upstream_count=upstream_count,
                downstream_count=downstream_count,
                influence_score=influence,
                is_hub=downstream_count >= hub_threshold,
            )
        )

    results.sort(key=lambda r: r.influence_score, reverse=True)
    return results
