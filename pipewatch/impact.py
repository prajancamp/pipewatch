"""Impact analysis: given a failed pipeline, find affected downstream pipelines."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Set

from pipewatch.pipeline_map import PipelineMap


@dataclass
class ImpactResult:
    root: str
    affected: List[str] = field(default_factory=list)
    depth: int = 0

    def __str__(self) -> str:
        if not self.affected:
            return f"No downstream impact from '{self.root}'."
        lines = [f"Impact from '{self.root}' (depth {self.depth}):"]
        for p in self.affected:
            lines.append(f"  - {p}")
        return "\n".join(lines)


def _walk_downstream(pm: PipelineMap, start: str, visited: Set[str], depth: int) -> int:
    """BFS over downstream nodes; returns max depth reached."""
    frontier = pm.get_downstream(start)
    max_depth = depth
    for node in frontier:
        if node not in visited:
            visited.add(node)
            d = _walk_downstream(pm, node, visited, depth + 1)
            max_depth = max(max_depth, d)
    return max_depth


def compute_impact(pm: PipelineMap, pipeline: str) -> ImpactResult:
    """Return all pipelines transitively downstream of *pipeline*."""
    visited: Set[str] = set()
    max_depth = _walk_downstream(pm, pipeline, visited, 1)
    affected = sorted(visited)
    return ImpactResult(root=pipeline, affected=affected, depth=max_depth if affected else 0)


def impact_summary(pm: PipelineMap, failed_pipelines: List[str]) -> List[ImpactResult]:
    """Compute impact for each failed pipeline (deduplicates affected lists)."""
    return [compute_impact(pm, p) for p in failed_pipelines]
