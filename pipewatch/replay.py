"""Replay stored pipeline runs through alert evaluation for retrospective analysis."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.models import PipelineRun
from pipewatch.alert import AlertRule, Alert, evaluate_alerts
from pipewatch.filter import filter_runs


@dataclass
class ReplayResult:
    pipeline: str
    total_runs: int
    alerts_fired: List[Alert] = field(default_factory=list)

    def __str__(self) -> str:
        lines = [f"[{self.pipeline}] replayed {self.total_runs} run(s), {len(self.alerts_fired)} alert(s) fired"]
        for a in self.alerts_fired:
            lines.append(f"  {a}")
        return "\n".join(lines)


def replay_pipeline(
    runs: List[PipelineRun],
    rules: List[AlertRule],
    pipeline: str,
) -> ReplayResult:
    """Evaluate alert rules against historical runs for a single pipeline."""
    matched = filter_runs(runs, pipeline=pipeline)
    alerts = evaluate_alerts(matched, rules)
    return ReplayResult(pipeline=pipeline, total_runs=len(matched), alerts_fired=alerts)


def replay_all(
    runs: List[PipelineRun],
    rules: List[AlertRule],
    pipelines: Optional[List[str]] = None,
) -> List[ReplayResult]:
    """Replay alert evaluation across all (or specified) pipelines."""
    if pipelines is None:
        pipelines = sorted({r.pipeline for r in runs})
    return [replay_pipeline(runs, rules, p) for p in pipelines]
