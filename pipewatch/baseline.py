"""Baseline management: store and compare pipeline performance baselines."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

DEFAULT_BASELINE_PATH = Path(".pipewatch") / "baseline.json"


@dataclass
class BaselineEntry:
    pipeline: str
    avg_duration: Optional[float]
    success_rate: float
    sample_size: int

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "BaselineEntry":
        return BaselineEntry(
            pipeline=d["pipeline"],
            avg_duration=d.get("avg_duration"),
            success_rate=d["success_rate"],
            sample_size=d["sample_size"],
        )


@dataclass
class BaselineDiff:
    pipeline: str
    success_rate_delta: float
    duration_delta: Optional[float]

    def __str__(self) -> str:
        parts = [f"{self.pipeline}:"]
        sign = "+" if self.success_rate_delta >= 0 else ""
        parts.append(f"  success_rate {sign}{self.success_rate_delta:.1%}")
        if self.duration_delta is not None:
            sign2 = "+" if self.duration_delta >= 0 else ""
            parts.append(f"  avg_duration {sign2}{self.duration_delta:.1f}s")
        return "\n".join(parts)


def save_baseline(entries: list[BaselineEntry], path: Path = DEFAULT_BASELINE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump([e.to_dict() for e in entries], f, indent=2)


def load_baseline(path: Path = DEFAULT_BASELINE_PATH) -> list[BaselineEntry]:
    if not path.exists():
        return []
    with open(path) as f:
        return [BaselineEntry.from_dict(d) for d in json.load(f)]


def diff_baseline(
    current: list[BaselineEntry], baseline: list[BaselineEntry]
) -> list[BaselineDiff]:
    base_map = {e.pipeline: e for e in baseline}
    diffs = []
    for entry in current:
        if entry.pipeline not in base_map:
            continue
        b = base_map[entry.pipeline]
        dur_delta = None
        if entry.avg_duration is not None and b.avg_duration is not None:
            dur_delta = entry.avg_duration - b.avg_duration
        diffs.append(BaselineDiff(
            pipeline=entry.pipeline,
            success_rate_delta=entry.success_rate - b.success_rate,
            duration_delta=dur_delta,
        ))
    return diffs
