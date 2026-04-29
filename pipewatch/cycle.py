"""Cycle detection: identify pipelines that repeat failure/success patterns cyclically."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class CycleResult:
    pipeline: str
    period: int          # detected cycle length in number of runs
    confidence: float    # 0.0 – 1.0
    sample_size: int
    pattern: List[str]   # e.g. ["success", "failed", "success", "failed"]

    def __str__(self) -> str:  # pragma: no cover
        bar = " → ".join(self.pattern)
        return (
            f"{self.pipeline}: period={self.period} "
            f"confidence={self.confidence:.0%} pattern=[{bar}]"
        )


def _outcome_sequence(runs: List[PipelineRun]) -> List[str]:
    """Return list of 'success'/'failed' ordered oldest-first."""
    sorted_runs = sorted(runs, key=lambda r: r.started_at or "")
    return ["success" if r.is_success() else "failed" for r in sorted_runs]


def _score_period(seq: List[str], period: int) -> float:
    """Return fraction of positions that match a repeating pattern of *period*."""
    if period < 2 or period > len(seq) // 2:
        return 0.0
    matches = sum(
        1 for i in range(period, len(seq)) if seq[i] == seq[i % period]
    )
    return matches / (len(seq) - period)


def detect_cycles(
    runs: List[PipelineRun],
    pipeline: Optional[str] = None,
    min_runs: int = 10,
    min_confidence: float = 0.75,
    max_period: int = 8,
) -> List[CycleResult]:
    """Detect repeating outcome cycles per pipeline."""
    from collections import defaultdict

    buckets: dict = defaultdict(list)
    for r in runs:
        if pipeline and r.pipeline != pipeline:
            continue
        buckets[r.pipeline].append(r)

    results: List[CycleResult] = []
    for name, pipe_runs in buckets.items():
        if len(pipe_runs) < min_runs:
            continue
        seq = _outcome_sequence(pipe_runs)
        best_period, best_score = 0, 0.0
        for p in range(2, min(max_period + 1, len(seq) // 2 + 1)):
            score = _score_period(seq, p)
            if score > best_score:
                best_score, best_period = score, p
        if best_score >= min_confidence and best_period >= 2:
            results.append(
                CycleResult(
                    pipeline=name,
                    period=best_period,
                    confidence=round(best_score, 4),
                    sample_size=len(seq),
                    pattern=seq[:best_period],
                )
            )
    return sorted(results, key=lambda r: r.confidence, reverse=True)
