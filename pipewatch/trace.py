"""Trace module: links pipeline runs into execution chains for end-to-end tracing."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.models import PipelineRun


@dataclass
class TraceChain:
    trace_id: str
    runs: List[PipelineRun] = field(default_factory=list)

    @property
    def pipelines(self) -> List[str]:
        return [r.pipeline for r in self.runs]

    @property
    def has_failure(self) -> bool:
        return any(r.is_failed() for r in self.runs)

    @property
    def total_duration(self) -> Optional[float]:
        durations = [r.duration for r in self.runs if r.duration is not None]
        return sum(durations) if durations else None

    @property
    def failed_runs(self) -> List[PipelineRun]:
        return [r for r in self.runs if r.is_failed()]

    def __str__(self) -> str:
        status = "FAIL" if self.has_failure else "OK"
        dur = f"{self.total_duration:.1f}s" if self.total_duration is not None else "n/a"
        return f"Trace[{self.trace_id}] pipelines={len(self.runs)} status={status} duration={dur}"


def build_traces(runs: List[PipelineRun]) -> Dict[str, TraceChain]:
    """Group runs by trace_id found in run.meta."""
    chains: Dict[str, TraceChain] = {}
    for run in runs:
        tid = (run.meta or {}).get("trace_id")
        if not tid:
            continue
        if tid not in chains:
            chains[tid] = TraceChain(trace_id=tid)
        chains[tid].runs.append(run)
    for chain in chains.values():
        chain.runs.sort(key=lambda r: r.started_at or "")
    return chains


def failing_traces(chains: Dict[str, TraceChain]) -> List[TraceChain]:
    """Return only chains that contain at least one failed run."""
    return [c for c in chains.values() if c.has_failure]


def trace_summary(chains: Dict[str, TraceChain]) -> Dict[str, int]:
    total = len(chains)
    failed = sum(1 for c in chains.values() if c.has_failure)
    return {"total": total, "failed": failed, "healthy": total - failed}
