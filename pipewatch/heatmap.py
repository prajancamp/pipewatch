"""Heatmap: failure frequency by hour-of-day and day-of-week."""
from __future__ import annotations
from dataclasses import dataclass, field
from collections import defaultdict
from typing import List, Dict
from pipewatch.models import PipelineRun, PipelineStatus

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


@dataclass
class HeatmapCell:
    day: str
    hour: int
    total: int
    failures: int

    @property
    def failure_rate(self) -> float:
        return self.failures / self.total if self.total else 0.0

    def __str__(self) -> str:
        return f"{self.day} {self.hour:02d}:xx  runs={self.total} failures={self.failures} rate={self.failure_rate:.0%}"


def compute_heatmap(
    runs: List[PipelineRun],
    pipeline: str | None = None,
) -> List[HeatmapCell]:
    """Return heatmap cells for each (day, hour) bucket with at least one run."""
    if pipeline:
        runs = [r for r in runs if r.pipeline == pipeline]

    total: Dict[tuple, int] = defaultdict(int)
    failures: Dict[tuple, int] = defaultdict(int)

    for run in runs:
        if run.started_at is None:
            continue
        key = (run.started_at.weekday(), run.started_at.hour)
        total[key] += 1
        if run.status == PipelineStatus.FAILED:
            failures[key] += 1

    cells = []
    for key in sorted(total):
        dow, hour = key
        cells.append(HeatmapCell(
            day=DAYS[dow],
            hour=hour,
            total=total[key],
            failures=failures.get(key, 0),
        ))
    return cells


def top_failure_slots(cells: List[HeatmapCell], n: int = 5) -> List[HeatmapCell]:
    """Return the n cells with the highest failure counts."""
    return sorted(cells, key=lambda c: c.failures, reverse=True)[:n]
