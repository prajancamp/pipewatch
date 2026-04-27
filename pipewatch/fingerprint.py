"""Failure fingerprinting — group and identify recurring failure patterns by signature.

A fingerprint is a stable hash derived from the pipeline name and a normalised
error message.  Runs that share a fingerprint are considered the same class of
failure, making it easy to track how often a specific error recurs, when it was
first and last seen, and which pipelines are affected.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from pipewatch.models import PipelineRun


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DIGITS_RE = re.compile(r"\d+")
_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I
)
_PATH_RE = re.compile(r"(/[\w./-]+)")


def _normalise_error(error: Optional[str]) -> str:
    """Strip volatile tokens (numbers, UUIDs, paths) for stable fingerprinting."""
    if not error:
        return ""
    text = _UUID_RE.sub("<uuid>", error)
    text = _PATH_RE.sub("<path>", text)
    text = _DIGITS_RE.sub("<n>", text)
    return text.lower().strip()


def _make_fingerprint(pipeline: str, error: Optional[str]) -> str:
    """Return a short hex fingerprint for a (pipeline, error) pair."""
    normalised = _normalise_error(error)
    raw = f"{pipeline}||{normalised}"
    return hashlib.sha1(raw.encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class FingerprintGroup:
    """Aggregated information about a single failure fingerprint."""

    fingerprint: str
    pipeline: str
    error_template: str          # normalised error used to build the hash
    sample_error: Optional[str]  # one real error message for display
    occurrences: int
    first_seen: datetime
    last_seen: datetime
    run_ids: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        return (
            f"[{self.fingerprint}] {self.pipeline} — "
            f"{self.occurrences} occurrence(s) | "
            f"first: {self.first_seen.strftime('%Y-%m-%d %H:%M')} | "
            f"last: {self.last_seen.strftime('%Y-%m-%d %H:%M')}\n"
            f"  error: {self.sample_error or '(no error message)'}"
        )


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def fingerprint_runs(
    runs: List[PipelineRun],
    pipeline: Optional[str] = None,
) -> Dict[str, FingerprintGroup]:
    """Compute fingerprint groups for all failed runs.

    Args:
        runs: All pipeline runs to analyse.
        pipeline: Optional pipeline name filter.

    Returns:
        Mapping of fingerprint hex string → FingerprintGroup, sorted by
        descending occurrence count.
    """
    groups: Dict[str, FingerprintGroup] = {}

    for run in runs:
        if not run.is_failed():
            continue
        if pipeline and run.pipeline != pipeline:
            continue

        fp = _make_fingerprint(run.pipeline, run.error)
        ts = run.started_at

        if fp not in groups:
            groups[fp] = FingerprintGroup(
                fingerprint=fp,
                pipeline=run.pipeline,
                error_template=_normalise_error(run.error),
                sample_error=run.error,
                occurrences=1,
                first_seen=ts,
                last_seen=ts,
                run_ids=[run.run_id],
            )
        else:
            g = groups[fp]
            g.occurrences += 1
            g.run_ids.append(run.run_id)
            if ts < g.first_seen:
                g.first_seen = ts
            if ts > g.last_seen:
                g.last_seen = ts
                g.sample_error = run.error  # keep the most recent sample

    # Sort by most occurrences first
    return dict(
        sorted(groups.items(), key=lambda kv: kv[1].occurrences, reverse=True)
    )


def top_fingerprints(
    runs: List[PipelineRun],
    limit: int = 10,
    pipeline: Optional[str] = None,
) -> List[FingerprintGroup]:
    """Return the *limit* most frequent failure fingerprints."""
    groups = fingerprint_runs(runs, pipeline=pipeline)
    return list(groups.values())[:limit]
