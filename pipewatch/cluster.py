"""Cluster pipeline runs by error message similarity."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict
from pipewatch.models import PipelineRun


@dataclass
class ErrorCluster:
    key: str
    runs: List[PipelineRun] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.runs)

    @property
    def pipelines(self) -> List[str]:
        return sorted({r.pipeline for r in self.runs})

    def __str__(self) -> str:
        return f"[{self.count} runs] '{self.key}' — pipelines: {', '.join(self.pipelines)}"


def _normalize_error(error: str) -> str:
    """Reduce an error string to a stable cluster key."""
    import re
    # strip hex addresses, numbers, UUIDs, file paths
    s = error.lower().strip()
    s = re.sub(r'0x[0-9a-f]+', '<addr>', s)
    s = re.sub(r'[0-9a-f]{8}-[0-9a-f\-]{27}', '<uuid>', s)
    s = re.sub(r'\b\d+\b', '<n>', s)
    s = re.sub(r'/[\w/\.\-]+', '<path>', s)
    s = re.sub(r'\s+', ' ', s)
    return s[:120]


def cluster_by_error(runs: List[PipelineRun]) -> List[ErrorCluster]:
    """Group failed runs by normalised error key."""
    buckets: Dict[str, ErrorCluster] = {}
    for run in runs:
        if not run.is_failed():
            continue
        raw = run.error or "(no error)"
        key = _normalize_error(raw)
        if key not in buckets:
            buckets[key] = ErrorCluster(key=key)
        buckets[key].runs.append(run)
    return sorted(buckets.values(), key=lambda c: -c.count)
