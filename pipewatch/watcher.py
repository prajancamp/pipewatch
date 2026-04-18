"""Watcher module: monitors a log/json file for new pipeline run entries."""

import time
import json
from pathlib import Path
from typing import Callable, Optional
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.store import RunStore


def _parse_line(line: str) -> Optional[PipelineRun]:
    """Parse a JSON log line into a PipelineRun, or return None on failure."""
    line = line.strip()
    if not line:
        return None
    try:
        data = json.loads(line)
        data["status"] = PipelineStatus(data["status"])
        return PipelineRun(**data)
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None


def tail_file(
    path: Path,
    store: RunStore,
    on_run: Optional[Callable[[PipelineRun], None]] = None,
    poll_interval: float = 1.0,
    max_iterations: Optional[int] = None,
) -> None:
    """
    Tail a newline-delimited JSON file, ingesting new PipelineRun entries.

    Each line should be a JSON object matching PipelineRun fields.
    Newly seen lines are appended to the store and optionally passed to on_run.

    Args:
        path: Path to the NDJSON log file to watch.
        store: RunStore instance to persist runs.
        on_run: Optional callback invoked with each new PipelineRun.
        poll_interval: Seconds between file polls.
        max_iterations: Stop after this many poll cycles (useful for testing).
    """
    path = Path(path)
    seen_lines = 0
    iterations = 0

    while True:
        if path.exists():
            with open(path, "r") as fh:
                lines = fh.readlines()

            new_lines = lines[seen_lines:]
            for line in new_lines:
                run = _parse_line(line)
                if run is not None:
                    store.append(run)
                    if on_run:
                        on_run(run)
            seen_lines = len(lines)

        iterations += 1
        if max_iterations is not None and iterations >= max_iterations:
            break
        time.sleep(poll_interval)
