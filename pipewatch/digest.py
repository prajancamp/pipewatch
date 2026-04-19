"""Daily/periodic digest summarizing pipeline health across all tracked pipelines."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

from pipewatch.analyzer import compute_stats, PipelineStats
from pipewatch.filter import filter_runs, unique_pipelines
from pipewatch.store import RunStore


@dataclass
class DigestReport:
    generated_at: str
    period_hours: int
    pipeline_count: int
    total_runs: int
    failed_pipelines: List[str]
    stats: List[PipelineStats]

    def __str__(self) -> str:
        lines = [
            f"=== PipeWatch Digest ({self.period_hours}h) — {self.generated_at} ===",
            f"Pipelines tracked : {self.pipeline_count}",
            f"Total runs        : {self.total_runs}",
        ]
        if self.failed_pipelines:
            lines.append(f"Failing pipelines : {', '.join(self.failed_pipelines)}")
        else:
            lines.append("Failing pipelines : none")
        lines.append("")
        for s in self.stats:
            lines.append(str(s))
        return "\n".join(lines)


def build_digest(store: RunStore, period_hours: int = 24) -> DigestReport:
    """Build a digest report covering the last *period_hours* hours."""
    since = datetime.utcnow() - timedelta(hours=period_hours)
    all_runs = store.load_all()
    recent = filter_runs(all_runs, since=since)

    pipelines = unique_pipelines(recent)
    stats_list: List[PipelineStats] = []
    failed: List[str] = []

    for name in sorted(pipelines):
        runs = [r for r in recent if r.pipeline_name == name]
        s = compute_stats(name, runs)
        stats_list.append(s)
        if s.success_rate is not None and s.success_rate < 1.0:
            failed.append(name)

    return DigestReport(
        generated_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        period_hours=period_hours,
        pipeline_count=len(pipelines),
        total_runs=len(recent),
        failed_pipelines=failed,
        stats=stats_list,
    )
