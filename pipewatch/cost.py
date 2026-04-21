"""Cost tracking and estimation for pipeline runs."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Dict

from pipewatch.models import PipelineRun


@dataclass
class CostEntry:
    pipeline: str
    run_id: str
    duration_seconds: Optional[float]
    cost_usd: Optional[float]

    def __str__(self) -> str:
        cost = f"${self.cost_usd:.4f}" if self.cost_usd is not None else "n/a"
        dur = f"{self.duration_seconds:.1f}s" if self.duration_seconds is not None else "n/a"
        return f"{self.pipeline} [{self.run_id[:8]}] duration={dur} cost={cost}"


@dataclass
class CostSummary:
    pipeline: str
    total_runs: int
    total_cost_usd: float
    avg_cost_usd: float
    total_duration_seconds: float

    def __str__(self) -> str:
        return (
            f"{self.pipeline}: runs={self.total_runs} "
            f"total=${self.total_cost_usd:.4f} avg=${self.avg_cost_usd:.4f} "
            f"duration={self.total_duration_seconds:.1f}s"
        )


def estimate_cost(
    run: PipelineRun,
    rate_per_second: float = 0.0001,
) -> CostEntry:
    """Estimate cost for a single run based on duration and a per-second rate."""
    cost: Optional[float] = None
    if run.duration is not None:
        cost = run.duration * rate_per_second
    return CostEntry(
        pipeline=run.pipeline,
        run_id=run.run_id,
        duration_seconds=run.duration,
        cost_usd=cost,
    )


def compute_cost_summary(
    runs: List[PipelineRun],
    rate_per_second: float = 0.0001,
) -> Dict[str, CostSummary]:
    """Aggregate cost estimates per pipeline."""
    buckets: Dict[str, List[CostEntry]] = {}
    for run in runs:
        entry = estimate_cost(run, rate_per_second)
        buckets.setdefault(run.pipeline, []).append(entry)

    summaries: Dict[str, CostSummary] = {}
    for pipeline, entries in buckets.items():
        costs = [e.cost_usd for e in entries if e.cost_usd is not None]
        durations = [e.duration_seconds for e in entries if e.duration_seconds is not None]
        total_cost = sum(costs)
        avg_cost = total_cost / len(costs) if costs else 0.0
        summaries[pipeline] = CostSummary(
            pipeline=pipeline,
            total_runs=len(entries),
            total_cost_usd=total_cost,
            avg_cost_usd=avg_cost,
            total_duration_seconds=sum(durations),
        )
    return summaries
