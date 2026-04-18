"""Alert rules and notification formatting for pipewatch."""
from dataclasses import dataclass
from typing import List, Optional
from pipewatch.models import PipelineRun
from pipewatch.analyzer import PipelineStats, find_consecutive_failures


@dataclass
class AlertRule:
    consecutive_failures: int = 3
    min_success_rate: float = 0.5
    max_avg_duration: Optional[float] = None


@dataclass
class Alert:
    pipeline: str
    level: str  # 'warning' | 'critical'
    message: str

    def __str__(self) -> str:
        icon = "⚠️ " if self.level == "warning" else "🔴"
        return f"{icon} [{self.level.upper()}] {self.pipeline}: {self.message}"


def evaluate_alerts(stats: List[PipelineStats], runs: List[PipelineRun], rule: AlertRule) -> List[Alert]:
    alerts: List[Alert] = []
    for s in stats:
        if s.success_rate < rule.min_success_rate:
            alerts.append(Alert(
                pipeline=s.pipeline,
                level="critical",
                message=f"Success rate {s.success_rate:.0%} below threshold {rule.min_success_rate:.0%}"
            ))
        if rule.max_avg_duration and s.avg_duration and s.avg_duration > rule.max_avg_duration:
            alerts.append(Alert(
                pipeline=s.pipeline,
                level="warning",
                message=f"Avg duration {s.avg_duration:.1f}s exceeds limit {rule.max_avg_duration:.1f}s"
            ))
    pipeline_runs = {}
    for r in runs:
        pipeline_runs.setdefault(r.pipeline, []).append(r)
    for pipeline, pruns in pipeline_runs.items():
        streak = find_consecutive_failures(pruns)
        if streak >= rule.consecutive_failures:
            alerts.append(Alert(
                pipeline=pipeline,
                level="critical",
                message=f"{streak} consecutive failures detected"
            ))
    return alerts
