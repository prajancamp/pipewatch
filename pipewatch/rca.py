"""Root cause analysis: surface likely causes for pipeline failures."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.models import PipelineRun


@dataclass
class RCAFinding:
    pipeline: str
    run_id: str
    error: Optional[str]
    causes: List[str] = field(default_factory=list)
    confidence: str = "low"  # low | medium | high

    def __str__(self) -> str:
        causes_str = "; ".join(self.causes) if self.causes else "unknown"
        return (
            f"[{self.confidence.upper()}] {self.pipeline}/{self.run_id}: {causes_str}"
        )


_TRANSIENT_PATTERNS = [
    ("timeout", "Transient timeout — consider retry"),
    ("connection", "Network/connection issue — may self-resolve"),
    ("rate limit", "Rate-limited by upstream service"),
    ("throttl", "Throttled by upstream service"),
]

_PERMANENT_PATTERNS = [
    ("permission", "Permission denied — check credentials/IAM"),
    ("not found", "Resource not found — check config or schema"),
    ("syntax", "Syntax error in pipeline code"),
    ("schema", "Schema mismatch — upstream data changed"),
    ("null", "Unexpected null value — data quality issue"),
    ("disk", "Disk/storage capacity issue"),
    ("memory", "Out-of-memory — consider scaling"),
]


def _match_patterns(error: str, patterns: list) -> List[str]:
    low = error.lower()
    return [msg for keyword, msg in patterns if keyword in low]


def analyze_run(run: PipelineRun) -> Optional[RCAFinding]:
    """Return an RCAFinding for a failed run, or None if the run succeeded."""
    if not run.is_failed():
        return None

    causes: List[str] = []
    error = run.error or ""

    causes += _match_patterns(error, _PERMANENT_PATTERNS)
    causes += _match_patterns(error, _TRANSIENT_PATTERNS)

    if not causes:
        causes = ["No recognisable pattern — inspect logs manually"]
        confidence = "low"
    elif len(causes) >= 2:
        confidence = "high"
    else:
        confidence = "medium"

    return RCAFinding(
        pipeline=run.pipeline,
        run_id=run.run_id,
        error=run.error,
        causes=causes,
        confidence=confidence,
    )


def analyze_all(runs: List[PipelineRun]) -> List[RCAFinding]:
    """Return RCA findings for every failed run in *runs*."""
    findings = []
    for run in runs:
        finding = analyze_run(run)
        if finding is not None:
            findings.append(finding)
    return findings
