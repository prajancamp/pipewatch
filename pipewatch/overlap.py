"""Detect pipelines whose execution windows overlap in time."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class OverlapResult:
    pipeline_a: str
    pipeline_b: str
    run_id_a: str
    run_id_b: str
    overlap_seconds: float
    started_at_a: str
    started_at_b: str

    def __str__(self) -> str:
        return (
            f"{self.pipeline_a} <-> {self.pipeline_b} "
            f"overlap={self.overlap_seconds:.1f}s "
            f"(runs {self.run_id_a[:8]} / {self.run_id_b[:8]})"
        )


def _run_interval(run: PipelineRun) -> Optional[tuple[float, float]]:
    """Return (start_ts, end_ts) in epoch seconds, or None if incomplete."""
    try:
        import datetime

        start = datetime.datetime.fromisoformat(run.started_at).timestamp()
        if run.finished_at is None:
            return None
        end = datetime.datetime.fromisoformat(run.finished_at).timestamp()
        if end < start:
            return None
        return start, end
    except Exception:
        return None


def detect_overlaps(
    runs: List[PipelineRun],
    pipeline: Optional[str] = None,
    min_overlap_seconds: float = 0.0,
) -> List[OverlapResult]:
    """Find pairs of runs from *different* pipelines whose time windows overlap."""
    if pipeline:
        runs = [r for r in runs if r.pipeline_name == pipeline]

    # build list of (run, start, end)
    timed = []
    for r in runs:
        interval = _run_interval(r)
        if interval is not None:
            timed.append((r, interval[0], interval[1]))

    results: List[OverlapResult] = []
    for i in range(len(timed)):
        for j in range(i + 1, len(timed)):
            ra, sa, ea = timed[i]
            rb, sb, eb = timed[j]
            if ra.pipeline_name == rb.pipeline_name:
                continue
            overlap = min(ea, eb) - max(sa, sb)
            if overlap > min_overlap_seconds:
                results.append(
                    OverlapResult(
                        pipeline_a=ra.pipeline_name,
                        pipeline_b=rb.pipeline_name,
                        run_id_a=ra.run_id,
                        run_id_b=rb.run_id,
                        overlap_seconds=overlap,
                        started_at_a=ra.started_at,
                        started_at_b=rb.started_at,
                    )
                )

    results.sort(key=lambda r: r.overlap_seconds, reverse=True)
    return results
