"""Signal detection: identify pipelines emitting consistent warning signals
based on recent run patterns (flapping, degrading, recovering)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class SignalResult:
    pipeline: str
    signal: str          # 'flapping' | 'degrading' | 'recovering' | 'stable'
    confidence: float    # 0.0 – 1.0
    detail: str

    def __str__(self) -> str:
        bar = int(self.confidence * 10)
        meter = "#" * bar + "." * (10 - bar)
        return f"{self.pipeline:<30} {self.signal:<12} [{meter}] {self.confidence:.0%}  {self.detail}"


def _recent_outcomes(runs: List[PipelineRun], window: int) -> List[bool]:
    """Return list of success booleans for the *window* most-recent runs."""
    sorted_runs = sorted(runs, key=lambda r: r.started_at)
    tail = sorted_runs[-window:]
    return [r.is_success() for r in tail]


def _transitions(outcomes: List[bool]) -> int:
    """Count how many times the outcome flips between consecutive runs."""
    return sum(1 for a, b in zip(outcomes, outcomes[1:]) if a != b)


def detect_signals(
    runs: List[PipelineRun],
    pipeline: Optional[str] = None,
    window: int = 10,
    min_runs: int = 4,
) -> List[SignalResult]:
    """Analyse recent run history and emit a signal for each pipeline."""
    if pipeline:
        runs = [r for r in runs if r.pipeline == pipeline]

    by_pipeline: dict[str, List[PipelineRun]] = {}
    for r in runs:
        by_pipeline.setdefault(r.pipeline, []).append(r)

    results: List[SignalResult] = []
    for name, pruns in sorted(by_pipeline.items()):
        if len(pruns) < min_runs:
            continue

        outcomes = _recent_outcomes(pruns, window)
        n = len(outcomes)
        successes = sum(outcomes)
        failures = n - successes
        rate = successes / n
        flips = _transitions(outcomes)

        if flips >= max(2, n // 2):
            signal = "flapping"
            confidence = min(1.0, flips / n)
            detail = f"{flips} transitions in last {n} runs"
        elif failures >= 3 and not outcomes[-1] and not outcomes[-2]:
            # recent tail is failures and getting worse
            recent_half = outcomes[n // 2 :]
            recent_fail_rate = 1 - sum(recent_half) / len(recent_half)
            early_half = outcomes[: n // 2]
            early_fail_rate = 1 - sum(early_half) / max(len(early_half), 1)
            if recent_fail_rate > early_fail_rate + 0.2:
                signal = "degrading"
                confidence = min(1.0, recent_fail_rate - early_fail_rate + 0.5)
                detail = f"failure rate rose from {early_fail_rate:.0%} to {recent_fail_rate:.0%}"
            else:
                signal = "stable"
                confidence = rate
                detail = f"{successes}/{n} recent runs succeeded"
        elif successes >= 3 and outcomes[-1] and outcomes[-2]:
            recent_half = outcomes[n // 2 :]
            recent_success_rate = sum(recent_half) / len(recent_half)
            early_half = outcomes[: n // 2]
            early_success_rate = sum(early_half) / max(len(early_half), 1)
            if recent_success_rate > early_success_rate + 0.2:
                signal = "recovering"
                confidence = min(1.0, recent_success_rate - early_success_rate + 0.5)
                detail = f"success rate rose from {early_success_rate:.0%} to {recent_success_rate:.0%}"
            else:
                signal = "stable"
                confidence = rate
                detail = f"{successes}/{n} recent runs succeeded"
        else:
            signal = "stable"
            confidence = rate
            detail = f"{successes}/{n} recent runs succeeded"

        results.append(SignalResult(name, signal, round(confidence, 2), detail))

    return results
