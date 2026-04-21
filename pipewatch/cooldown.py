"""Cooldown tracking: suppress repeated alerts for a pipeline within a time window."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional


def _cooldown_path(store_path: str) -> Path:
    return Path(store_path).parent / ".pipewatch_cooldowns.json"


def load_cooldown_state(store_path: str) -> Dict[str, str]:
    """Return mapping of pipeline -> ISO timestamp of last alert."""
    path = _cooldown_path(store_path)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def save_cooldown_state(store_path: str, state: Dict[str, str]) -> None:
    path = _cooldown_path(store_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2))


def is_in_cooldown(
    store_path: str,
    pipeline: str,
    cooldown_minutes: int = 30,
    now: Optional[datetime] = None,
) -> bool:
    """Return True if pipeline fired an alert within the cooldown window."""
    state = load_cooldown_state(store_path)
    if pipeline not in state:
        return False
    now = now or datetime.utcnow()
    last_fired = datetime.fromisoformat(state[pipeline])
    return (now - last_fired) < timedelta(minutes=cooldown_minutes)


def record_cooldown(
    store_path: str,
    pipeline: str,
    now: Optional[datetime] = None,
) -> None:
    """Record that an alert was fired for pipeline right now."""
    state = load_cooldown_state(store_path)
    now = now or datetime.utcnow()
    state[pipeline] = now.isoformat()
    save_cooldown_state(store_path, state)


def clear_cooldown(store_path: str, pipeline: str) -> None:
    """Remove cooldown entry for a pipeline."""
    state = load_cooldown_state(store_path)
    state.pop(pipeline, None)
    save_cooldown_state(store_path, state)


@dataclass
class CooldownStatus:
    pipeline: str
    in_cooldown: bool
    last_fired: Optional[datetime]
    cooldown_minutes: int

    def __str__(self) -> str:
        if self.in_cooldown:
            return (
                f"{self.pipeline}: IN COOLDOWN (last fired {self.last_fired.isoformat()}, "
                f"window={self.cooldown_minutes}m)"
            )
        return f"{self.pipeline}: ready"


def check_cooldown_status(
    store_path: str,
    pipeline: str,
    cooldown_minutes: int = 30,
    now: Optional[datetime] = None,
) -> CooldownStatus:
    state = load_cooldown_state(store_path)
    now = now or datetime.utcnow()
    last_fired: Optional[datetime] = None
    if pipeline in state:
        last_fired = datetime.fromisoformat(state[pipeline])
    in_cd = is_in_cooldown(store_path, pipeline, cooldown_minutes, now=now)
    return CooldownStatus(
        pipeline=pipeline,
        in_cooldown=in_cd,
        last_fired=last_fired,
        cooldown_minutes=cooldown_minutes,
    )
