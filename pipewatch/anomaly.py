from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
from pipewatch.models import PipelineRun
from pipewatch.analyzer import compute_stats


@dataclass
class AnomalyResult:
    pipeline: str
    run_id: str
    reason: str
    duration: Optional[float]
    avg_duration: Optional[float]

    def __str__(self) -> str:
        return f"[ANOMALY] {self.pipeline} run={self.run_id}: {self.reason}"


def _mean(values: List[float]) -> float:
    return sum(values) / len(values)


def _stddev(values: List[float], mean: float) -> float:
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return variance ** 0.5


def detect_duration_anomalies(
    runs: List[PipelineRun],
    z_threshold: float = 2.5,
) -> List[AnomalyResult]:
    """Flag runs whose duration is more than z_threshold std-devs from the mean."""
    from pipewatch.filter import filter_runs

    anomalies: List[AnomalyResult] = []
    stats_list = compute_stats(runs)

    for stats in stats_list:
        pipeline_runs = [r for r in runs if r.pipeline == stats.pipeline and r.duration is not None]
        durations = [r.duration for r in pipeline_runs]  # type: ignore[misc]
        if len(durations) < 3:
            continue
        mean = _mean(durations)
        std = _stddev(durations, mean)
        if std == 0:
            continue
        for run in pipeline_runs:
            z = abs((run.duration - mean) / std)  # type: ignore[operator]
            if z > z_threshold:
                anomalies.append(
                    AnomalyResult(
                        pipeline=run.pipeline,
                        run_id=run.run_id,
                        reason=f"duration {run.duration:.1f}s is {z:.2f} std-devs from mean {mean:.1f}s",
                        duration=run.duration,
                        avg_duration=mean,
                    )
                )
    return anomalies


def detect_anomalies(runs: List[PipelineRun], z_threshold: float = 2.5) -> List[AnomalyResult]:
    return detect_duration_anomalies(runs, z_threshold=z_threshold)
