"""Quorum: detect pipelines that require agreement across multiple runs before
marking a status as authoritative (e.g. avoid flipping on a single blip)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.models import PipelineRun, PipelineStatus


@dataclass
class QuorumResult:
    pipeline: str
    window: int          # number of most-recent runs evaluated
    required: int        # votes needed to reach quorum
    failure_votes: int
    success_votes: int
    quorum_status: Optional[PipelineStatus]   # None = no quorum reached
    confident: bool

    def __str__(self) -> str:  # pragma: no cover
        status = self.quorum_status.value if self.quorum_status else "undecided"
        return (
            f"{self.pipeline}: {status} "
            f"(success={self.success_votes}/{self.window}, "
            f"failure={self.failure_votes}/{self.window}, "
            f"required={self.required})"
        )


def _majority_status(
    runs: List[PipelineRun],
    required: int,
) -> tuple[int, int, Optional[PipelineStatus], bool]:
    """Return (failure_votes, success_votes, quorum_status, confident)."""
    failure_votes = sum(1 for r in runs if r.is_failed())
    success_votes = sum(1 for r in runs if r.is_success())

    if failure_votes >= required:
        return failure_votes, success_votes, PipelineStatus.FAILED, True
    if success_votes >= required:
        return failure_votes, success_votes, PipelineStatus.SUCCESS, True
    return failure_votes, success_votes, None, False


def check_quorum(
    runs: List[PipelineRun],
    *,
    pipeline: Optional[str] = None,
    window: int = 5,
    required: int = 3,
) -> List[QuorumResult]:
    """Evaluate quorum for each pipeline (or a single one) over the last
    *window* runs, requiring *required* matching votes."""
    if required > window:
        raise ValueError("required cannot exceed window")

    from pipewatch.filter import filter_runs, unique_pipelines

    targets = [pipeline] if pipeline else unique_pipelines(runs)
    results: List[QuorumResult] = []

    for name in sorted(targets):
        pipe_runs = filter_runs(runs, pipeline=name)
        recent = sorted(pipe_runs, key=lambda r: r.started_at or "")[-window:]
        fv, sv, status, confident = _majority_status(recent, required)
        results.append(
            QuorumResult(
                pipeline=name,
                window=len(recent),
                required=required,
                failure_votes=fv,
                success_votes=sv,
                quorum_status=status,
                confident=confident,
            )
        )
    return results
