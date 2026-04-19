"""Alert throttling: suppress repeated alerts within a cooldown window."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, Optional


def _throttle_path(store_path: Path) -> Path:
    return store_path / ".throttle.json"


def load_throttle_state(store_path: Path) -> Dict[str, float]:
    """Load last-fired timestamps keyed by alert key."""
    p = _throttle_path(store_path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def save_throttle_state(store_path: Path, state: Dict[str, float]) -> None:
    p = _throttle_path(store_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, indent=2))


def is_throttled(
    key: str,
    state: Dict[str, float],
    cooldown_seconds: float,
    now: Optional[float] = None,
) -> bool:
    """Return True if the alert identified by *key* is within its cooldown."""
    if key not in state:
        return False
    last = state[key]
    current = now if now is not None else time.time()
    return (current - last) < cooldown_seconds


def record_fired(
    key: str,
    state: Dict[str, float],
    now: Optional[float] = None,
) -> Dict[str, float]:
    """Mark *key* as fired at *now*, returning the updated state dict."""
    updated = dict(state)
    updated[key] = now if now is not None else time.time()
    return updated


def filter_throttled_alerts(
    alert_keys: list[str],
    store_path: Path,
    cooldown_seconds: float = 3600.0,
    now: Optional[float] = None,
) -> tuple[list[str], Dict[str, float]]:
    """Return only unthrottled alert keys and updated throttle state."""
    state = load_throttle_state(store_path)
    active: list[str] = []
    for key in alert_keys:
        if not is_throttled(key, state, cooldown_seconds, now):
            active.append(key)
            state = record_fired(key, state, now)
    return active, state
