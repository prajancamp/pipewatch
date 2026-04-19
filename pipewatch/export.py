"""Export pipeline runs to various formats."""

from __future__ import annotations

import csv
import json
import io
from typing import List, Optional

from pipewatch.models import PipelineRun
from pipewatch.filter import filter_runs


def runs_to_dicts(runs: List[PipelineRun]) -> List[dict]:
    return [r.to_dict() for r in runs]


def export_runs_json(
    runs: List[PipelineRun],
    pipeline: Optional[str] = None,
    status: Optional[str] = None,
) -> str:
    filtered = filter_runs(runs, pipeline=pipeline, status=status)
    return json.dumps(runs_to_dicts(filtered), indent=2)


def export_runs_csv(
    runs: List[PipelineRun],
    pipeline: Optional[str] = None,
    status: Optional[str] = None,
) -> str:
    filtered = filter_runs(runs, pipeline=pipeline, status=status)
    if not filtered:
        return ""
    fields = ["run_id", "pipeline", "status", "started_at", "ended_at", "duration", "error", "tags"]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for row in runs_to_dicts(filtered):
        row["tags"] = "|".join(row.get("tags") or [])
        writer.writerow(row)
    return buf.getvalue()


def write_export(
    runs: List[PipelineRun],
    path: str,
    fmt: str = "json",
    pipeline: Optional[str] = None,
    status: Optional[str] = None,
) -> int:
    """Write exported runs to a file. Returns number of runs written."""
    filtered = filter_runs(runs, pipeline=pipeline, status=status)
    if fmt == "json":
        content = export_runs_json(filtered)
    elif fmt == "csv":
        content = export_runs_csv(filtered)
    else:
        raise ValueError(f"Unsupported format: {fmt}")
    with open(path, "w") as f:
        f.write(content)
    return len(filtered)
