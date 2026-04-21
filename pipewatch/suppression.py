"""Suppression rules: silence alerts for known/expected failure patterns."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from pipewatch.alert import Alert


def _suppression_path(store_path: str) -> Path:
    return Path(store_path).parent / "suppressions.json"


@dataclass
class SuppressionRule:
    pipeline: Optional[str] = None   # None means match any pipeline
    alert_type: Optional[str] = None  # None means match any type
    reason: str = ""

    def matches(self, alert: Alert) -> bool:
        if self.pipeline and alert.pipeline != self.pipeline:
            return False
        if self.alert_type and alert.rule.name != self.alert_type:
            return False
        return True

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "alert_type": self.alert_type,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "SuppressionRule":
        return cls(
            pipeline=d.get("pipeline"),
            alert_type=d.get("alert_type"),
            reason=d.get("reason", ""),
        )

    def __str__(self) -> str:
        parts = []
        if self.pipeline:
            parts.append(f"pipeline={self.pipeline}")
        if self.alert_type:
            parts.append(f"type={self.alert_type}")
        label = ", ".join(parts) if parts else "*"
        return f"SuppressionRule({label}, reason={self.reason!r})"


def load_rules(store_path: str) -> List[SuppressionRule]:
    path = _suppression_path(store_path)
    if not path.exists():
        return []
    with path.open() as fh:
        data = json.load(fh)
    return [SuppressionRule.from_dict(d) for d in data]


def save_rules(store_path: str, rules: List[SuppressionRule]) -> None:
    path = _suppression_path(store_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump([r.to_dict() for r in rules], fh, indent=2)


def add_rule(store_path: str, rule: SuppressionRule) -> None:
    rules = load_rules(store_path)
    rules.append(rule)
    save_rules(store_path, rules)


def suppress_alerts(
    alerts: List[Alert], rules: List[SuppressionRule]
) -> tuple[List[Alert], List[Alert]]:
    """Return (active_alerts, suppressed_alerts)."""
    active, suppressed = [], []
    for alert in alerts:
        if any(r.matches(alert) for r in rules):
            suppressed.append(alert)
        else:
            active.append(alert)
    return active, suppressed
