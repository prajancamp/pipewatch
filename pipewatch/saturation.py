"""Saturation analysis: detect pipelines running near or over capacity limits."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class SaturationResult:
    pipeline: str
    window_hours: int
    run_count: int
    max_runs: int
    utilization: float          # 0.0 – 1.0+
    avg_duration_s: Optional[float]
    is_saturated: bool

    def __str__(self) -> str:
        pct = f"{self.utilization * 100:.1f}%"
        status = "SATURATED" if self.is_saturated else "ok"
        dur = (
            f"{self.avg_duration_s:.1f}s" if self.avg_duration_s is not None else "n/a"
        )
        return (
            f"{self.pipeline}: {self.run_count}/{self.max_runs} runs "
            f"in {self.window_hours}h ({pct}) avg={dur} [{status}]"
        )


def _runs_in_window(
    runs: List[PipelineRun], pipeline: str, window_hours: int, now: float
) -> List[PipelineRun]:
    cutoff = now - window_hours * 3600
    return [
        r for r in runs
        if r.pipeline == pipeline and r.started_at >= cutoff
    ]


def check_saturation(
    runs: List[PipelineRun],
    max_runs: int,
    window_hours: int = 1,
    pipeline: Optional[str] = None,
    *,
    _now: Optional[float] = None,
) -> List[SaturationResult]:
    """Return a SaturationResult per pipeline (optionally filtered)."""
    import time

    now = _now if _now is not None else time.time()

    pipelines = (
        [pipeline]
        if pipeline
        else sorted({r.pipeline for r in runs})
    )

    results: List[SaturationResult] = []
    for name in pipelines:
        window_runs = _runs_in_window(runs, name, window_hours, now)
        count = len(window_runs)

        durations = [
            r.duration for r in window_runs if r.duration is not None
        ]
        avg_dur = sum(durations) / len(durations) if durations else None

        utilization = count / max_runs if max_runs > 0 else 0.0
        results.append(
            SaturationResult(
                pipeline=name,
                window_hours=window_hours,
                run_count=count,
                max_runs=max_runs,
                utilization=utilization,
                avg_duration_s=avg_dur,
                is_saturated=count >= max_runs,
            )
        )
    return results
