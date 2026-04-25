"""Burndown: track open failure incidents over time and measure resolution rate."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class BurndownPoint:
    """A single point in a burndown chart."""
    timestamp: datetime
    open_failures: int
    resolved: int
    pipeline: Optional[str] = None

    def __str__(self) -> str:
        pipe = f"[{self.pipeline}] " if self.pipeline else ""
        ts = self.timestamp.strftime("%Y-%m-%d %H:%M")
        return f"{pipe}{ts}  open={self.open_failures}  resolved={self.resolved}"


@dataclass
class BurndownReport:
    pipeline: Optional[str]
    points: List[BurndownPoint]
    total_opened: int
    total_resolved: int

    @property
    def resolution_rate(self) -> float:
        if self.total_opened == 0:
            return 1.0
        return round(self.total_resolved / self.total_opened, 4)

    def __str__(self) -> str:
        pipe = self.pipeline or "(all pipelines)"
        lines = [
            f"Burndown — {pipe}",
            f"  Opened : {self.total_opened}",
            f"  Resolved: {self.total_resolved}",
            f"  Resolution rate: {self.resolution_rate:.1%}",
        ]
        for pt in self.points:
            lines.append(f"  {pt}")
        return "\n".join(lines)


def compute_burndown(
    runs: List[PipelineRun],
    pipeline: Optional[str] = None,
) -> BurndownReport:
    """Compute a burndown report from a list of pipeline runs.

    Each failure opens an incident; the next success on the same pipeline
    closes (resolves) it.
    """
    if pipeline:
        runs = [r for r in runs if r.pipeline == pipeline]

    sorted_runs = sorted(runs, key=lambda r: r.started_at)

    open_count = 0
    resolved_count = 0
    total_opened = 0
    total_resolved = 0
    points: List[BurndownPoint] = []

    # Track open incidents per pipeline
    open_per_pipeline: dict[str, int] = {}

    for run in sorted_runs:
        name = run.pipeline
        if run.is_failed():
            open_per_pipeline[name] = open_per_pipeline.get(name, 0) + 1
            total_opened += 1
        elif run.is_success() and open_per_pipeline.get(name, 0) > 0:
            open_per_pipeline[name] -= 1
            total_resolved += 1

        open_count = sum(open_per_pipeline.values())
        points.append(
            BurndownPoint(
                timestamp=run.started_at,
                open_failures=open_count,
                resolved=total_resolved,
                pipeline=run.pipeline if not pipeline else None,
            )
        )

    return BurndownReport(
        pipeline=pipeline,
        points=points,
        total_opened=total_opened,
        total_resolved=total_resolved,
    )
