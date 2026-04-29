"""Heartbeat tracking: detect pipelines that have stopped reporting entirely."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class HeartbeatResult:
    pipeline: str
    last_seen: Optional[datetime]
    silence_minutes: Optional[float]
    expected_interval_minutes: float
    is_dead: bool

    def __str__(self) -> str:
        if self.last_seen is None:
            return f"{self.pipeline}: NEVER SEEN (expected every {self.expected_interval_minutes:.0f}m)"
        status = "DEAD" if self.is_dead else "OK"
        return (
            f"{self.pipeline}: {status} "
            f"(last seen {self.silence_minutes:.1f}m ago, "
            f"expected every {self.expected_interval_minutes:.0f}m)"
        )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def check_heartbeat(
    runs: List[PipelineRun],
    pipeline: str,
    expected_interval_minutes: float = 60.0,
    grace_factor: float = 2.0,
) -> HeartbeatResult:
    """Check whether a single pipeline has missed its expected heartbeat."""
    pipeline_runs = [r for r in runs if r.pipeline == pipeline]
    if not pipeline_runs:
        return HeartbeatResult(
            pipeline=pipeline,
            last_seen=None,
            silence_minutes=None,
            expected_interval_minutes=expected_interval_minutes,
            is_dead=True,
        )

    latest = max(pipeline_runs, key=lambda r: r.started_at)
    now = _now()
    started = latest.started_at
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    silence = (now - started).total_seconds() / 60.0
    threshold = expected_interval_minutes * grace_factor
    return HeartbeatResult(
        pipeline=pipeline,
        last_seen=started,
        silence_minutes=silence,
        expected_interval_minutes=expected_interval_minutes,
        is_dead=silence > threshold,
    )


def check_all_heartbeats(
    runs: List[PipelineRun],
    expected_interval_minutes: float = 60.0,
    grace_factor: float = 2.0,
) -> List[HeartbeatResult]:
    """Check heartbeats for every distinct pipeline in *runs*."""
    pipelines = sorted({r.pipeline for r in runs})
    return [
        check_heartbeat(runs, p, expected_interval_minutes, grace_factor)
        for p in pipelines
    ]
