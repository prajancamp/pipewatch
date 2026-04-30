"""Mirror module: compare two stores or pipeline subsets side-by-side."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pipewatch.analyzer import PipelineStats, compute_stats
from pipewatch.models import PipelineRun


@dataclass
class MirrorEntry:
    pipeline: str
    left_success_rate: Optional[float]
    right_success_rate: Optional[float]
    left_total: int
    right_total: int
    left_avg_duration: Optional[float]
    right_avg_duration: Optional[float]

    @property
    def success_rate_delta(self) -> Optional[float]:
        if self.left_success_rate is None or self.right_success_rate is None:
            return None
        return self.right_success_rate - self.left_success_rate

    @property
    def duration_delta(self) -> Optional[float]:
        if self.left_avg_duration is None or self.right_avg_duration is None:
            return None
        return self.right_avg_duration - self.left_avg_duration

    def __str__(self) -> str:
        sr_left = f"{self.left_success_rate:.1%}" if self.left_success_rate is not None else "N/A"
        sr_right = f"{self.right_success_rate:.1%}" if self.right_success_rate is not None else "N/A"
        delta = self.success_rate_delta
        delta_str = f"{delta:+.1%}" if delta is not None else "N/A"
        return (
            f"{self.pipeline}: left={sr_left} ({self.left_total} runs) "
            f"right={sr_right} ({self.right_total} runs) delta={delta_str}"
        )


def _stats_index(runs: List[PipelineRun]) -> Dict[str, PipelineStats]:
    return {s.pipeline: s for s in compute_stats(runs)}


def compute_mirror(
    left_runs: List[PipelineRun],
    right_runs: List[PipelineRun],
    pipeline: Optional[str] = None,
) -> List[MirrorEntry]:
    """Compare stats for all pipelines present in either run set."""
    left_idx = _stats_index(left_runs)
    right_idx = _stats_index(right_runs)

    all_pipelines = sorted(set(left_idx) | set(right_idx))
    if pipeline:
        all_pipelines = [p for p in all_pipelines if p == pipeline]

    results: List[MirrorEntry] = []
    for name in all_pipelines:
        ls = left_idx.get(name)
        rs = right_idx.get(name)
        results.append(
            MirrorEntry(
                pipeline=name,
                left_success_rate=ls.success_rate if ls else None,
                right_success_rate=rs.success_rate if rs else None,
                left_total=ls.total_runs if ls else 0,
                right_total=rs.total_runs if rs else 0,
                left_avg_duration=ls.avg_duration if ls else None,
                right_avg_duration=rs.avg_duration if rs else None,
            )
        )
    return results
