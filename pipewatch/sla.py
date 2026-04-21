"""SLA (Service Level Agreement) tracking for pipeline runs."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class SLAResult:
    pipeline: str
    total_runs: int
    breaches: int
    breach_rate: float  # 0.0 – 1.0
    max_duration: Optional[float]
    threshold: float  # seconds

    def __str__(self) -> str:
        pct = f"{self.breach_rate * 100:.1f}%"
        return (
            f"{self.pipeline}: {self.breaches}/{self.total_runs} breaches "
            f"({pct}) — threshold {self.threshold:.0f}s"
        )

    @property
    def is_breaching(self) -> bool:
        """True when at least one breach occurred."""
        return self.breaches > 0


def _completed_runs(runs: List[PipelineRun]) -> List[PipelineRun]:
    """Return only runs that have a valid non-negative duration."""
    return [r for r in runs if r.duration_seconds is not None and r.duration_seconds >= 0]


def check_sla(
    runs: List[PipelineRun],
    threshold: float,
    pipeline: Optional[str] = None,
) -> List[SLAResult]:
    """Compute SLA breach statistics per pipeline.

    Args:
        runs: All pipeline runs to evaluate.
        threshold: Maximum allowed duration in seconds.
        pipeline: If given, restrict to this pipeline name.

    Returns:
        One :class:`SLAResult` per distinct pipeline found in *runs*.
    """
    if pipeline:
        runs = [r for r in runs if r.pipeline == pipeline]

    pipelines: dict[str, List[PipelineRun]] = {}
    for run in runs:
        pipelines.setdefault(run.pipeline, []).append(run)

    results: List[SLAResult] = []
    for name, pipe_runs in sorted(pipelines.items()):
        completed = _completed_runs(pipe_runs)
        if not completed:
            results.append(
                SLAResult(
                    pipeline=name,
                    total_runs=0,
                    breaches=0,
                    breach_rate=0.0,
                    max_duration=None,
                    threshold=threshold,
                )
            )
            continue

        durations = [r.duration_seconds for r in completed]  # type: ignore[misc]
        breaches = sum(1 for d in durations if d > threshold)
        max_dur = max(durations)
        results.append(
            SLAResult(
                pipeline=name,
                total_runs=len(completed),
                breaches=breaches,
                breach_rate=breaches / len(completed),
                max_duration=max_dur,
                threshold=threshold,
            )
        )
    return results
