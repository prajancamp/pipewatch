"""Assess pipeline health considering upstream dependency failures."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.pipeline_map import PipelineMap
from pipewatch.health import HealthStatus, assess_health
from pipewatch.models import PipelineRun


@dataclass
class DependencyHealthResult:
    pipeline: str
    own_health: HealthStatus
    upstream_issues: List[str] = field(default_factory=list)
    blocked_by: List[str] = field(default_factory=list)

    @property
    def is_blocked(self) -> bool:
        return len(self.blocked_by) > 0

    def __str__(self) -> str:
        lines = [f"[{self.pipeline}] own={self.own_health.level}"]
        if self.blocked_by:
            lines.append(f"  blocked-by: {', '.join(self.blocked_by)}")
        for issue in self.upstream_issues:
            lines.append(f"  upstream-issue: {issue}")
        return "\n".join(lines)


def assess_dependency_health(
    pipeline: str,
    all_runs: List[PipelineRun],
    pipeline_map: PipelineMap,
    consecutive_threshold: int = 2,
) -> DependencyHealthResult:
    """Check a pipeline's health alongside its upstream dependencies."""
    own_runs = [r for r in all_runs if r.pipeline == pipeline]
    own_health = assess_health(own_runs)

    upstream = pipeline_map.upstream_of(pipeline)
    upstream_issues: List[str] = []
    blocked_by: List[str] = []

    for up in upstream:
        up_runs = [r for r in all_runs if r.pipeline == up]
        if not up_runs:
            continue
        up_health = assess_health(up_runs)
        if up_health.level == "CRITICAL":
            blocked_by.append(up)
        elif up_health.level == "WARN":
            upstream_issues.append(f"{up} is degraded ({up_health.level})")

    return DependencyHealthResult(
        pipeline=pipeline,
        own_health=own_health,
        upstream_issues=upstream_issues,
        blocked_by=blocked_by,
    )


def assess_all_dependency_health(
    all_runs: List[PipelineRun],
    pipeline_map: PipelineMap,
) -> List[DependencyHealthResult]:
    """Assess dependency health for every known pipeline."""
    pipelines = pipeline_map.all_pipelines()
    results = []
    for p in pipelines:
        results.append(assess_dependency_health(p, all_runs, pipeline_map))
    return results
