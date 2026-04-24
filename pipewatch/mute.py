"""Mute rules: silence alerts for specific pipelines or patterns for a duration."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


def _mute_path(store_path: str) -> Path:
    return Path(store_path).parent / "mute_rules.json"


@dataclass
class MuteRule:
    pipeline: str          # exact name or glob-style pattern (fnmatch)
    reason: str
    expires_at: Optional[str]  # ISO-8601 or None (permanent)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return datetime.fromisoformat(self.expires_at) < datetime.now(timezone.utc)

    def matches(self, pipeline_name: str) -> bool:
        import fnmatch
        return fnmatch.fnmatch(pipeline_name, self.pipeline)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "reason": self.reason,
            "expires_at": self.expires_at,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "MuteRule":
        return cls(
            pipeline=d["pipeline"],
            reason=d["reason"],
            expires_at=d.get("expires_at"),
            created_at=d.get("created_at", ""),
        )

    def __str__(self) -> str:
        exp = self.expires_at or "permanent"
        return f"MuteRule(pipeline={self.pipeline!r}, reason={self.reason!r}, expires={exp})"


def load_mute_rules(store_path: str) -> List[MuteRule]:
    p = _mute_path(store_path)
    if not p.exists():
        return []
    with p.open() as f:
        return [MuteRule.from_dict(d) for d in json.load(f)]


def save_mute_rules(store_path: str, rules: List[MuteRule]) -> None:
    p = _mute_path(store_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as f:
        json.dump([r.to_dict() for r in rules], f, indent=2)


def add_mute_rule(store_path: str, rule: MuteRule) -> None:
    rules = load_mute_rules(store_path)
    rules.append(rule)
    save_mute_rules(store_path, rules)


def remove_expired_rules(store_path: str) -> int:
    rules = load_mute_rules(store_path)
    active = [r for r in rules if not r.is_expired()]
    removed = len(rules) - len(active)
    save_mute_rules(store_path, active)
    return removed


def is_muted(pipeline_name: str, rules: List[MuteRule]) -> bool:
    return any(r.matches(pipeline_name) and not r.is_expired() for r in rules)
