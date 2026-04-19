"""Watchdog: detect pipelines that haven't run within an expected interval."""
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional
from pipewatch.models import PipelineRun
from pipewatch.filter import latest_run_per_pipeline


@dataclass
class StaleAlert:
    pipeline: str
    last_seen: datetime
    stale_after_minutes: int

    def age_minutes(self) -> float:
        now = datetime.now(timezone.utc)
        last = self.last_seen
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        return (now - last).total_seconds() / 60

    def __str__(self) -> str:
        return (
            f"[STALE] {self.pipeline} — last run {self.age_minutes():.1f}m ago "
            f"(threshold: {self.stale_after_minutes}m)"
        )


def find_stale_pipelines(
    runs: List[PipelineRun],
    stale_after_minutes: int = 60,
    pipeline_thresholds: Optional[dict] = None,
) -> List[StaleAlert]:
    """Return stale alerts for pipelines whose latest run is older than threshold."""
    latest = latest_run_per_pipeline(runs)
    alerts: List[StaleAlert] = []
    for run in latest:
        threshold = stale_after_minutes
        if pipeline_thresholds and run.pipeline in pipeline_thresholds:
            threshold = pipeline_thresholds[run.pipeline]
        ts = run.started_at
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age_minutes = (datetime.now(timezone.utc) - ts).total_seconds() / 60
        if age_minutes > threshold:
            alerts.append(StaleAlert(
                pipeline=run.pipeline,
                last_seen=ts,
                stale_after_minutes=threshold,
            ))
    return alerts
