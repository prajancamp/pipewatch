"""Watermark tracking: record and compare high-water marks for pipeline metrics."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from pipewatch.models import PipelineRun


@dataclass
class WatermarkEntry:
    pipeline: str
    metric: str          # 'success_rate' | 'avg_duration' | 'run_count'
    value: float
    recorded_at: str     # ISO timestamp of the run that set the mark

    def __str__(self) -> str:
        return f"{self.pipeline} [{self.metric}] = {self.value:.3f} (set {self.recorded_at})"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "value": self.value,
            "recorded_at": self.recorded_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "WatermarkEntry":
        return cls(
            pipeline=d["pipeline"],
            metric=d["metric"],
            value=d["value"],
            recorded_at=d["recorded_at"],
        )


def _watermark_path(store_path: Path) -> Path:
    return store_path.parent / "watermarks.json"


def load_watermarks(store_path: Path) -> List[WatermarkEntry]:
    p = _watermark_path(store_path)
    if not p.exists():
        return []
    with p.open() as f:
        return [WatermarkEntry.from_dict(d) for d in json.load(f)]


def save_watermarks(store_path: Path, entries: List[WatermarkEntry]) -> None:
    p = _watermark_path(store_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as f:
        json.dump([e.to_dict() for e in entries], f, indent=2)


def compute_watermarks(runs: List[PipelineRun]) -> List[WatermarkEntry]:
    """Compute per-pipeline high-water marks from a list of runs."""
    from collections import defaultdict

    buckets: Dict[str, List[PipelineRun]] = defaultdict(list)
    for r in runs:
        buckets[r.pipeline].append(r)

    results: List[WatermarkEntry] = []
    for pipeline, pruns in sorted(buckets.items()):
        total = len(pruns)
        successes = sum(1 for r in pruns if r.is_success())
        rate = successes / total if total else 0.0
        durations = [r.duration for r in pruns if r.duration is not None]
        avg_dur = sum(durations) / len(durations) if durations else 0.0
        latest_ts = max(r.started_at for r in pruns)

        results.append(WatermarkEntry(pipeline, "success_rate", rate, latest_ts))
        results.append(WatermarkEntry(pipeline, "run_count", float(total), latest_ts))
        if durations:
            results.append(WatermarkEntry(pipeline, "avg_duration", avg_dur, latest_ts))

    return results


def update_watermarks(store_path: Path, runs: List[PipelineRun]) -> List[WatermarkEntry]:
    """Merge newly computed marks with stored ones, keeping the best value."""
    existing = {(e.pipeline, e.metric): e for e in load_watermarks(store_path)}
    fresh = compute_watermarks(runs)

    for entry in fresh:
        key = (entry.pipeline, entry.metric)
        prev = existing.get(key)
        if prev is None or entry.value >= prev.value:
            existing[key] = entry

    updated = sorted(existing.values(), key=lambda e: (e.pipeline, e.metric))
    save_watermarks(store_path, updated)
    return updated
