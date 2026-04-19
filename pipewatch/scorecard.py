from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.analyzer import PipelineStats


@dataclass
class ScorecardEntry:
    pipeline: str
    score: float  # 0.0 - 100.0
    grade: str
    reasons: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        reasons_str = "; ".join(self.reasons) if self.reasons else "No issues"
        return f"{self.pipeline}: {self.grade} ({self.score:.1f}) — {reasons_str}"


def _grade(score: float) -> str:
    if score >= 90:
        return "A"
    elif score >= 75:
        return "B"
    elif score >= 60:
        return "C"
    elif score >= 40:
        return "D"
    return "F"


def score_pipeline(stats: PipelineStats) -> ScorecardEntry:
    score = 100.0
    reasons: List[str] = []

    if stats.total_runs == 0:
        return ScorecardEntry(pipeline=stats.pipeline, score=0.0, grade="F", reasons=["No runs recorded"])

    success_rate = stats.success_rate
    if success_rate < 0.5:
        penalty = 40
        score -= penalty
        reasons.append(f"Low success rate ({success_rate:.0%})")
    elif success_rate < 0.8:
        penalty = 20
        score -= penalty
        reasons.append(f"Below-target success rate ({success_rate:.0%})")

    if stats.consecutive_failures >= 5:
        score -= 25
        reasons.append(f"{stats.consecutive_failures} consecutive failures")
    elif stats.consecutive_failures >= 3:
        score -= 10
        reasons.append(f"{stats.consecutive_failures} consecutive failures")

    if stats.avg_duration_seconds is not None and stats.avg_duration_seconds > 3600:
        score -= 10
        reasons.append(f"High avg duration ({stats.avg_duration_seconds:.0f}s)")

    score = max(0.0, min(100.0, score))
    return ScorecardEntry(pipeline=stats.pipeline, score=score, grade=_grade(score), reasons=reasons)


def build_scorecard(stats_list: List[PipelineStats]) -> List[ScorecardEntry]:
    entries = [score_pipeline(s) for s in stats_list]
    return sorted(entries, key=lambda e: e.score)
