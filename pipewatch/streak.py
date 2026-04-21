"""Streak tracking: longest success and failure streaks per pipeline."""

from dataclasses import dataclass
from typing import List, Dict
from pipewatch.models import PipelineRun, PipelineStatus


@dataclass
class StreakResult:
    pipeline: str
    current_streak_type: str  # 'success' | 'failure' | 'none'
    current_streak_length: int
    longest_success_streak: int
    longest_failure_streak: int

    def __str__(self) -> str:
        return (
            f"{self.pipeline}: current={self.current_streak_type}x{self.current_streak_length} "
            f"best_success={self.longest_success_streak} "
            f"worst_failure={self.longest_failure_streak}"
        )


def _compute_streak(runs: List[PipelineRun]) -> StreakResult:
    """Compute streak stats for a single pipeline's runs (oldest-first)."""
    if not runs:
        return StreakResult(
            pipeline=runs[0].pipeline if runs else "",
            current_streak_type="none",
            current_streak_length=0,
            longest_success_streak=0,
            longest_failure_streak=0,
        )

    sorted_runs = sorted(runs, key=lambda r: r.started_at)
    pipeline = sorted_runs[0].pipeline

    longest_success = 0
    longest_failure = 0
    cur_success = 0
    cur_failure = 0

    for run in sorted_runs:
        if run.is_success():
            cur_success += 1
            cur_failure = 0
        elif run.is_failed():
            cur_failure += 1
            cur_success = 0
        else:
            cur_success = 0
            cur_failure = 0

        longest_success = max(longest_success, cur_success)
        longest_failure = max(longest_failure, cur_failure)

    # Determine current streak from the trailing end
    last = sorted_runs[-1]
    if last.is_success():
        current_type = "success"
        current_len = cur_success
    elif last.is_failed():
        current_type = "failure"
        current_len = cur_failure
    else:
        current_type = "none"
        current_len = 0

    return StreakResult(
        pipeline=pipeline,
        current_streak_type=current_type,
        current_streak_length=current_len,
        longest_success_streak=longest_success,
        longest_failure_streak=longest_failure,
    )


def compute_streaks(runs: List[PipelineRun]) -> Dict[str, StreakResult]:
    """Compute streak results for all pipelines in the run list."""
    grouped: Dict[str, List[PipelineRun]] = {}
    for run in runs:
        grouped.setdefault(run.pipeline, []).append(run)

    return {pipeline: _compute_streak(pipeline_runs) for pipeline, pipeline_runs in grouped.items()}
