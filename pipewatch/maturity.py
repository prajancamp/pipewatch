"""Pipeline maturity scoring based on run history stability."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.models import PipelineRun
from pipewatch.analyzer import compute_stats


_LEVELS = ["infant", "developing", "stable", "mature", "veteran"]


@dataclass
class MaturityResult:
    pipeline: str
    score: float          # 0.0 – 100.0
    level: str
    total_runs: int
    success_rate: float
    avg_duration: Optional[float]
    consecutive_failures: int

    def __str__(self) -> str:
        dur = f"{self.avg_duration:.1f}s" if self.avg_duration is not None else "n/a"
        return (
            f"{self.pipeline}: {self.level.upper()} "
            f"(score={self.score:.1f}, runs={self.total_runs}, "
            f"success={self.success_rate*100:.1f}%, dur={dur})"
        )


def _level(score: float) -> str:
    """Map a 0-100 score to a named maturity level."""
    if score >= 90:
        return _LEVELS[4]
    if score >= 75:
        return _LEVELS[3]
    if score >= 55:
        return _LEVELS[2]
    if score >= 35:
        return _LEVELS[1]
    return _LEVELS[0]


def score_maturity(pipeline: str, runs: List[PipelineRun]) -> Optional[MaturityResult]:
    """Compute a maturity score for a single pipeline from its run history."""
    pipeline_runs = [r for r in runs if r.pipeline == pipeline]
    if not pipeline_runs:
        return None

    stats = compute_stats(pipeline_runs)[pipeline]

    # --- component scores (each 0-100) ---
    # 1. Volume: log-scale up to 100 runs
    import math
    volume = min(math.log1p(stats.total_runs) / math.log1p(100), 1.0) * 100

    # 2. Success rate
    success = stats.success_rate * 100

    # 3. Consecutive failure penalty
    cf_penalty = min(stats.consecutive_failures * 10, 40)

    raw = (volume * 0.25) + (success * 0.75) - cf_penalty
    score = max(0.0, min(100.0, raw))

    return MaturityResult(
        pipeline=pipeline,
        score=round(score, 2),
        level=_level(score),
        total_runs=stats.total_runs,
        success_rate=stats.success_rate,
        avg_duration=stats.avg_duration,
        consecutive_failures=stats.consecutive_failures,
    )


def build_maturity_report(runs: List[PipelineRun]) -> List[MaturityResult]:
    """Return maturity results for every pipeline found in *runs*."""
    pipelines = sorted({r.pipeline for r in runs})
    results = [score_maturity(p, runs) for p in pipelines]
    return [r for r in results if r is not None]
