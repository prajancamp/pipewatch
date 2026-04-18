"""Analyze pipeline run history for failure patterns and statistics."""
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional

from pipewatch.models import PipelineRun


@dataclass
class PipelineStats:
    pipeline_name: str
    total_runs: int
    failed_runs: int
    success_runs: int
    failure_rate: float
    avg_duration_seconds: Optional[float]
    last_status: Optional[str]

    def __str__(self) -> str:
        return (
            f"{self.pipeline_name}: {self.total_runs} runs, "
            f"{self.failure_rate:.1%} failure rate, "
            f"avg duration {self.avg_duration_seconds:.1f}s"
            if self.avg_duration_seconds is not None
            else f"{self.pipeline_name}: {self.total_runs} runs, "
                 f"{self.failure_rate:.1%} failure rate"
        )


def compute_stats(runs: List[PipelineRun]) -> Dict[str, PipelineStats]:
    """Compute per-pipeline statistics from a list of runs."""
    grouped: Dict[str, List[PipelineRun]] = defaultdict(list)
    for run in runs:
        grouped[run.pipeline_name].append(run)

    stats: Dict[str, PipelineStats] = {}
    for name, pipeline_runs in grouped.items():
        sorted_runs = sorted(pipeline_runs, key=lambda r: r.started_at)
        failed = [r for r in sorted_runs if r.is_failed()]
        succeeded = [r for r in sorted_runs if r.is_success()]
        durations = [
            r.duration_seconds
            for r in sorted_runs
            if r.duration_seconds is not None
        ]
        avg_duration = sum(durations) / len(durations) if durations else None
        stats[name] = PipelineStats(
            pipeline_name=name,
            total_runs=len(sorted_runs),
            failed_runs=len(failed),
            success_runs=len(succeeded),
            failure_rate=len(failed) / len(sorted_runs) if sorted_runs else 0.0,
            avg_duration_seconds=avg_duration,
            last_status=sorted_runs[-1].status.value if sorted_runs else None,
        )
    return stats


def find_consecutive_failures(runs: List[PipelineRun], threshold: int = 3) -> List[str]:
    """Return pipeline names with >= threshold consecutive failures (most recent)."""
    grouped: Dict[str, List[PipelineRun]] = defaultdict(list)
    for run in runs:
        grouped[run.pipeline_name].append(run)

    flagged = []
    for name, pipeline_runs in grouped.items():
        sorted_runs = sorted(pipeline_runs, key=lambda r: r.started_at, reverse=True)
        streak = 0
        for run in sorted_runs:
            if run.is_failed():
                streak += 1
            else:
                break
        if streak >= threshold:
            flagged.append(name)
    return flagged
