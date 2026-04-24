"""Triage module: prioritise failing pipelines for operator attention."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.analyzer import PipelineStats, compute_stats
from pipewatch.models import PipelineRun


_PRIORITY_LABELS = {3: "CRITICAL", 2: "HIGH", 1: "MEDIUM", 0: "LOW"}


@dataclass
class TriageResult:
    pipeline: str
    priority: int          # 0-3
    score: float
    reasons: List[str]

    @property
    def label(self) -> str:
        return _PRIORITY_LABELS.get(self.priority, "LOW")

    def __str__(self) -> str:
        reason_str = "; ".join(self.reasons) if self.reasons else "no issues"
        return f"[{self.label}] {self.pipeline} (score={self.score:.2f}) — {reason_str}"


def _score_stats(stats: PipelineStats) -> tuple[float, List[str]]:
    """Return (score, reasons) for a single pipeline's stats."""
    score = 0.0
    reasons: List[str] = []

    if stats.success_rate is not None:
        if stats.success_rate < 0.5:
            score += 3.0
            reasons.append(f"success rate {stats.success_rate:.0%}")
        elif stats.success_rate < 0.8:
            score += 1.5
            reasons.append(f"degraded success rate {stats.success_rate:.0%}")

    if stats.consecutive_failures >= 5:
        score += 3.0
        reasons.append(f"{stats.consecutive_failures} consecutive failures")
    elif stats.consecutive_failures >= 3:
        score += 1.5
        reasons.append(f"{stats.consecutive_failures} consecutive failures")

    if stats.total_runs == 0:
        score = 0.0

    return score, reasons


def _priority_from_score(score: float) -> int:
    if score >= 5.0:
        return 3
    if score >= 3.0:
        return 2
    if score >= 1.0:
        return 1
    return 0


def triage_runs(
    runs: List[PipelineRun],
    min_priority: int = 0,
    pipeline: Optional[str] = None,
) -> List[TriageResult]:
    """Compute triage results for all (or a specific) pipeline."""
    all_stats = compute_stats(runs)
    if pipeline:
        all_stats = [s for s in all_stats if s.pipeline == pipeline]

    results: List[TriageResult] = []
    for stats in all_stats:
        score, reasons = _score_stats(stats)
        priority = _priority_from_score(score)
        if priority >= min_priority:
            results.append(
                TriageResult(
                    pipeline=stats.pipeline,
                    priority=priority,
                    score=score,
                    reasons=reasons,
                )
            )

    results.sort(key=lambda r: (-r.priority, -r.score))
    return results
