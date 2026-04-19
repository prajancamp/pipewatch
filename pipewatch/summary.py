"""Summary export utilities for pipewatch pipeline reports."""
from __future__ import annotations

import csv
import io
import json
from typing import List

from pipewatch.analyzer import PipelineStats


def stats_to_dict(stats: PipelineStats) -> dict:
    return {
        "pipeline": stats.pipeline,
        "total_runs": stats.total_runs,
        "success_count": stats.success_count,
        "failure_count": stats.failure_count,
        "success_rate": round(stats.success_rate, 4),
        "avg_duration": round(stats.avg_duration, 2) if stats.avg_duration is not None else None,
        "last_status": stats.last_status.value if stats.last_status else None,
    }


def export_json(stats_list: List[PipelineStats]) -> str:
    """Serialize a list of PipelineStats to a JSON string."""
    return json.dumps([stats_to_dict(s) for s in stats_list], indent=2)


def export_csv(stats_list: List[PipelineStats]) -> str:
    """Serialize a list of PipelineStats to a CSV string."""
    fields = ["pipeline", "total_runs", "success_count", "failure_count",
              "success_rate", "avg_duration", "last_status"]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields)
    writer.writeheader()
    for s in stats_list:
        writer.writerow(stats_to_dict(s))
    return buf.getvalue()


def export_summary(stats_list: List[PipelineStats], fmt: str = "json") -> str:
    """Export summary in the requested format ('json' or 'csv')."""
    if fmt == "csv":
        return export_csv(stats_list)
    elif fmt == "json":
        return export_json(stats_list)
    else:
        raise ValueError(f"Unsupported format: {fmt!r}. Choose 'json' or 'csv'.")
