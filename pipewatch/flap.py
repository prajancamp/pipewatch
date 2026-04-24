"""Flap detection: identify pipelines that oscillate between success and failure."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class FlapResult:
    pipeline: str
    total_runs: int
    transitions: int          # number of status flips
    flap_rate: float          # transitions / (total_runs - 1)
    is_flapping: bool
    last_statuses: List[str]  # last N status strings for display

    def __str__(self) -> str:
        status_str = " -> ".join(self.last_statuses[-6:])
        flag = "[FLAPPING]" if self.is_flapping else "[stable]"
        return (
            f"{flag} {self.pipeline}: {self.transitions} transitions "
            f"over {self.total_runs} runs (flap_rate={self.flap_rate:.2f}) | {status_str}"
        )


def _count_transitions(statuses: List[str]) -> int:
    """Count how many times adjacent statuses differ."""
    return sum(1 for a, b in zip(statuses, statuses[1:]) if a != b)


def detect_flaps(
    runs: List[PipelineRun],
    pipeline: Optional[str] = None,
    min_runs: int = 4,
    flap_threshold: float = 0.5,
) -> List[FlapResult]:
    """
    Detect pipelines whose success/failure status flips frequently.

    Args:
        runs: All pipeline runs to analyse.
        pipeline: If given, restrict analysis to this pipeline.
        min_runs: Minimum number of runs required before flagging.
        flap_threshold: Flap rate (transitions / gaps) above which a pipeline
                        is considered flapping.

    Returns:
        List of FlapResult, one per pipeline, sorted by flap_rate descending.
    """
    from collections import defaultdict

    buckets: dict[str, List[PipelineRun]] = defaultdict(list)
    for run in runs:
        if pipeline and run.pipeline != pipeline:
            continue
        buckets[run.pipeline].append(run)

    results: List[FlapResult] = []
    for name, pipe_runs in buckets.items():
        sorted_runs = sorted(pipe_runs, key=lambda r: r.started_at)
        if len(sorted_runs) < min_runs:
            continue
        statuses = [r.status.value for r in sorted_runs]
        transitions = _count_transitions(statuses)
        rate = transitions / (len(statuses) - 1)
        results.append(
            FlapResult(
                pipeline=name,
                total_runs=len(sorted_runs),
                transitions=transitions,
                flap_rate=round(rate, 4),
                is_flapping=rate >= flap_threshold,
                last_statuses=statuses,
            )
        )

    results.sort(key=lambda r: r.flap_rate, reverse=True)
    return results
