"""Archive old runs to a compressed file and remove them from the active store."""

from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from pipewatch.models import PipelineRun
from pipewatch.store import RunStore


def _archive_path(store_path: Path, label: str) -> Path:
    archive_dir = store_path.parent / "archives"
    archive_dir.mkdir(parents=True, exist_ok=True)
    return archive_dir / f"{label}.jsonl.gz"


def archive_before(store: RunStore, cutoff: datetime, label: str | None = None) -> Path:
    """Move runs older than *cutoff* into a gzipped archive file."""
    all_runs = store.load_all()
    to_archive = [r for r in all_runs if r.started_at < cutoff]
    to_keep = [r for r in all_runs if r.started_at >= cutoff]

    if not label:
        label = cutoff.strftime("%Y%m%dT%H%M%S")

    dest = _archive_path(Path(store.path), label)
    with gzip.open(dest, "wt", encoding="utf-8") as fh:
        for run in to_archive:
            fh.write(json.dumps(run.to_dict()) + "\n")

    # Rewrite active store with only kept runs
    store_file = Path(store.path)
    with store_file.open("w", encoding="utf-8") as fh:
        for run in to_keep:
            fh.write(json.dumps(run.to_dict()) + "\n")

    return dest


def load_archive(archive_path: Path) -> List[PipelineRun]:
    """Read runs back from a gzipped archive file."""
    runs: List[PipelineRun] = []
    with gzip.open(archive_path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                runs.append(PipelineRun.from_dict(json.loads(line)))
    return runs


def list_archives(store_path: Path) -> List[Path]:
    """Return all archive files for a given store directory."""
    archive_dir = store_path.parent / "archives"
    if not archive_dir.exists():
        return []
    return sorted(archive_dir.glob("*.jsonl.gz"))
