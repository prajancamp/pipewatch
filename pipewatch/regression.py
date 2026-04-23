"""Detect pipelines that have regressed compared to a recent baseline window."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class RegressionResult:
    pipeline: str
    baseline_success_rate: float
    recent_success_rate: float
    baseline_avg_duration: Optional[float]
    recent_avg_duration: Optional[float]
    regressed: bool
    reason: str

    def __str__(self) -> str:
        flag = "[REGRESSED]" if self.regressed else "[OK]"
        return (
            f"{flag} {self.pipeline}: "
            f"success {self.baseline_success_rate:.0%} -> {self.recent_success_rate:.0%}  "
            f"duration {self.baseline_avg_duration} -> {self.recent_avg_duration}s  "
            f"({self.reason})"
        )


def _success_rate(runs: List[PipelineRun]) -> float:
    if not runs:
        return 1.0
    return sum(1 for r in runs if r.is_success()) / len(runs)


def _avg_duration(runs: List[PipelineRun]) -> Optional[float]:
    durations = [r.duration for r in runs if r.duration is not None]
    if not durations:
        return None
    return round(sum(durations) / len(durations), 2)


def detect_regression(
    runs: List[PipelineRun],
    baseline_window: int = 20,
    recent_window: int = 10,
    success_rate_drop: float = 0.15,
    duration_increase_pct: float = 0.25,
    pipeline: Optional[str] = None,
) -> List[RegressionResult]:
    """Compare the most recent *recent_window* runs against the preceding
    *baseline_window* runs for each pipeline and flag regressions."""
    if pipeline:
        runs = [r for r in runs if r.pipeline == pipeline]

    pipelines: dict[str, List[PipelineRun]] = {}
    for run in runs:
        pipelines.setdefault(run.pipeline, []).append(run)

    results: List[RegressionResult] = []
    for name, pipe_runs in sorted(pipelines.items()):
        ordered = sorted(pipe_runs, key=lambda r: r.started_at)
        if len(ordered) < recent_window + 1:
            continue

        baseline_slice = ordered[-(baseline_window + recent_window): -recent_window]
        recent_slice = ordered[-recent_window:]

        if not baseline_slice:
            continue

        base_sr = _success_rate(baseline_slice)
        rec_sr = _success_rate(recent_slice)
        base_dur = _avg_duration(baseline_slice)
        rec_dur = _avg_duration(recent_slice)

        reasons = []
        if base_sr - rec_sr >= success_rate_drop:
            reasons.append(
                f"success rate dropped {base_sr - rec_sr:.0%}"
            )
        if base_dur and rec_dur and rec_dur > base_dur * (1 + duration_increase_pct):
            reasons.append(
                f"avg duration up {((rec_dur / base_dur) - 1):.0%}"
            )

        regressed = bool(reasons)
        reason = "; ".join(reasons) if reasons else "within normal range"
        results.append(
            RegressionResult(
                pipeline=name,
                baseline_success_rate=base_sr,
                recent_success_rate=rec_sr,
                baseline_avg_duration=base_dur,
                recent_avg_duration=rec_dur,
                regressed=regressed,
                reason=reason,
            )
        )
    return results
