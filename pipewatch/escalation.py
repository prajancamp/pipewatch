"""Escalation: track repeated alert firing and suggest escalation actions."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.alert import Alert


ESCALATION_THRESHOLDS = {
    "low": 1,
    "medium": 3,
    "high": 5,
}


@dataclass
class EscalationResult:
    pipeline: str
    alert_count: int
    level: str  # "low", "medium", "high"
    alerts: List[Alert] = field(default_factory=list)
    suggestion: Optional[str] = None

    def __str__(self) -> str:
        parts = [
            f"[{self.level.upper()}] {self.pipeline}: {self.alert_count} alert(s)",
        ]
        if self.suggestion:
            parts.append(f"  Suggestion: {self.suggestion}")
        for a in self.alerts:
            parts.append(f"  - {a}")
        return "\n".join(parts)


def _escalation_level(count: int) -> str:
    if count >= ESCALATION_THRESHOLDS["high"]:
        return "high"
    if count >= ESCALATION_THRESHOLDS["medium"]:
        return "medium"
    return "low"


def _suggestion(level: str) -> Optional[str]:
    return {
        "low": "Monitor the pipeline closely.",
        "medium": "Notify the on-call engineer.",
        "high": "Page the team immediately and consider disabling the pipeline.",
    }.get(level)


def escalate_alerts(alerts: List[Alert]) -> List[EscalationResult]:
    """Group alerts by pipeline and compute escalation level."""
    grouped: dict[str, List[Alert]] = {}
    for alert in alerts:
        grouped.setdefault(alert.pipeline, []).append(alert)

    results: List[EscalationResult] = []
    for pipeline, pipeline_alerts in sorted(grouped.items()):
        count = len(pipeline_alerts)
        level = _escalation_level(count)
        results.append(
            EscalationResult(
                pipeline=pipeline,
                alert_count=count,
                level=level,
                alerts=pipeline_alerts,
                suggestion=_suggestion(level),
            )
        )
    return results
