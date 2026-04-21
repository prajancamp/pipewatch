"""Feedback loop: track resolved/acknowledged alerts and suppress future noise."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


def _feedback_path(store_path: str) -> Path:
    return Path(store_path).parent / "feedback.json"


@dataclass
class FeedbackEntry:
    alert_key: str          # e.g. "my_pipeline:consecutive_failures"
    action: str             # "acknowledged" | "resolved" | "suppressed"
    note: Optional[str]
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        return {
            "alert_key": self.alert_key,
            "action": self.action,
            "note": self.note,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "FeedbackEntry":
        return cls(
            alert_key=d["alert_key"],
            action=d["action"],
            note=d.get("note"),
            timestamp=d["timestamp"],
        )

    def __str__(self) -> str:
        note_part = f" — {self.note}" if self.note else ""
        return f"[{self.action.upper()}] {self.alert_key} @ {self.timestamp}{note_part}"


def load_feedback(store_path: str) -> List[FeedbackEntry]:
    path = _feedback_path(store_path)
    if not path.exists():
        return []
    with path.open() as fh:
        return [FeedbackEntry.from_dict(d) for d in json.load(fh)]


def save_feedback(store_path: str, entries: List[FeedbackEntry]) -> None:
    path = _feedback_path(store_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        json.dump([e.to_dict() for e in entries], fh, indent=2)


def add_feedback(store_path: str, alert_key: str, action: str, note: Optional[str] = None) -> FeedbackEntry:
    entries = load_feedback(store_path)
    entry = FeedbackEntry(alert_key=alert_key, action=action, note=note)
    entries.append(entry)
    save_feedback(store_path, entries)
    return entry


def suppressed_keys(store_path: str) -> Dict[str, FeedbackEntry]:
    """Return the most recent feedback entry per alert_key where action is 'suppressed'."""
    result: Dict[str, FeedbackEntry] = {}
    for entry in load_feedback(store_path):
        if entry.action == "suppressed":
            result[entry.alert_key] = entry
        elif entry.alert_key in result:
            # A later non-suppressed entry lifts the suppression
            del result[entry.alert_key]
    return result


def is_suppressed(store_path: str, alert_key: str) -> bool:
    return alert_key in suppressed_keys(store_path)
