"""Incident tracking: group consecutive failures into named incidents."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class Incident:
    pipeline: str
    runs: List[PipelineRun] = field(default_factory=list)

    @property
    def start(self) -> Optional[str]:
        return self.runs[0].started_at if self.runs else None

    @property
    def end(self) -> Optional[str]:
        return self.runs[-1].started_at if self.runs else None

    @property
    def length(self) -> int:
        return len(self.runs)

    @property
    def errors(self) -> List[str]:
        return [r.error for r in self.runs if r.error]

    def __str__(self) -> str:
        return (
            f"Incident({self.pipeline!r}, {self.length} failures, "
            f"from {self.start} to {self.end})"
        )


def detect_incidents(
    runs: List[PipelineRun],
    min_length: int = 2,
) -> List[Incident]:
    """Return incidents (consecutive failure runs) per pipeline."""
    from pipewatch.filter import filter_runs
    from collections import defaultdict

    by_pipeline: dict[str, List[PipelineRun]] = defaultdict(list)
    for r in sorted(runs, key=lambda x: x.started_at):
        by_pipeline[r.pipeline].append(r)

    incidents: List[Incident] = []
    for pipeline, pruns in by_pipeline.items():
        current: List[PipelineRun] = []
        for run in pruns:
            if run.is_failed():
                current.append(run)
            else:
                if len(current) >= min_length:
                    incidents.append(Incident(pipeline=pipeline, runs=list(current)))
                current = []
        if len(current) >= min_length:
            incidents.append(Incident(pipeline=pipeline, runs=list(current)))
    return incidents
