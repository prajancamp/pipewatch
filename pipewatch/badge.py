"""Badge generation for pipeline health status."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pipewatch.models import PipelineRun
from pipewatch.analyzer import compute_stats


@dataclass
class BadgeResult:
    pipeline: str
    label: str
    message: str
    color: str

    def __str__(self) -> str:
        return f"[{self.pipeline}] {self.label}: {self.message} ({self.color})"

    def to_shields_url(self) -> str:
        """Return a shields.io badge URL."""
        label = self.label.replace(" ", "_").replace("-", "--")
        message = self.message.replace(" ", "_").replace("-", "--")
        return (
            f"https://img.shields.io/badge/{label}-{message}-{self.color}"
        )


def _color_for_rate(success_rate: float) -> str:
    if success_rate >= 0.95:
        return "brightgreen"
    if success_rate >= 0.80:
        return "yellow"
    if success_rate >= 0.60:
        return "orange"
    return "red"


def generate_badge(pipeline: str, runs: List[PipelineRun]) -> BadgeResult:
    """Generate a health badge for a single pipeline."""
    pipeline_runs = [r for r in runs if r.pipeline == pipeline]
    if not pipeline_runs:
        return BadgeResult(
            pipeline=pipeline,
            label="pipeline",
            message="no data",
            color="lightgrey",
        )

    stats = compute_stats(pipeline_runs)[pipeline]
    rate = stats.success_rate if stats.success_rate is not None else 0.0
    pct = f"{rate * 100:.0f}%"
    color = _color_for_rate(rate)
    return BadgeResult(
        pipeline=pipeline,
        label="pipeline",
        message=pct,
        color=color,
    )


def generate_all_badges(runs: List[PipelineRun]) -> List[BadgeResult]:
    """Generate badges for every pipeline found in runs."""
    pipelines = sorted({r.pipeline for r in runs})
    return [generate_badge(p, runs) for p in pipelines]
