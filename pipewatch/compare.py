"""Compare pipeline run stats across two time windows."""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

from pipewatch.models import PipelineRun
from pipewatch.analyzer import compute_stats, PipelineStats


@dataclass
class WindowComparison:
    pipeline: str
    before_success_rate: Optional[float]
    after_success_rate: Optional[float]
    before_avg_duration: Optional[float]
    after_avg_duration: Optional[float]
    before_total: int
    after_total: int

    @property
    def success_rate_delta(self) -> Optional[float]:
        if self.before_success_rate is None or self.after_success_rate is None:
            return None
        return self.after_success_rate - self.before_success_rate

    @property
    def duration_delta(self) -> Optional[float]:
        if self.before_avg_duration is None or self.after_avg_duration is None:
            return None
        return self.after_avg_duration - self.before_avg_duration

    def __str__(self) -> str:
        sr = f"{self.success_rate_delta:+.1f}%" if self.success_rate_delta is not None else "n/a"
        dur = f"{self.duration_delta:+.1f}s" if self.duration_delta is not None else "n/a"
        return (
            f"[{self.pipeline}] success_rate: {sr}  avg_duration: {dur}  "
            f"runs: {self.before_total} -> {self.after_total}"
        )


def _filter_window(runs: List[PipelineRun], start: datetime, end: datetime) -> List[PipelineRun]:
    return [r for r in runs if start <= r.started_at < end]


def compare_windows(
    runs: List[PipelineRun],
    before_start: datetime,
    before_end: datetime,
    after_start: datetime,
    after_end: datetime,
) -> List[WindowComparison]:
    before_runs = _filter_window(runs, before_start, before_end)
    after_runs = _filter_window(runs, after_start, after_end)

    pipelines = {r.pipeline for r in before_runs} | {r.pipeline for r in after_runs}

    before_stats = {s.pipeline: s for s in compute_stats(before_runs)}
    after_stats = {s.pipeline: s for s in compute_stats(after_runs)}

    results = []
    for pipeline in sorted(pipelines):
        b = before_stats.get(pipeline)
        a = after_stats.get(pipeline)
        results.append(WindowComparison(
            pipeline=pipeline,
            before_success_rate=b.success_rate if b else None,
            after_success_rate=a.success_rate if a else None,
            before_avg_duration=b.avg_duration if b else None,
            after_avg_duration=a.avg_duration if a else None,
            before_total=b.total_runs if b else 0,
            after_total=a.total_runs if a else 0,
        ))
    return results
