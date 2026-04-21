"""CLI subcommands for the pulse (heartbeat) feature."""
from __future__ import annotations

import argparse
from typing import List

from pipewatch.pulse import compute_all_pulses, silent_pipelines
from pipewatch.store import RunStore


def cmd_pulse(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs recorded.")
        return

    pulses = compute_all_pulses(runs)

    if getattr(args, "silent_only", False):
        targets = silent_pipelines(pulses)
        if not targets:
            print("All pipelines are active within the last 24 hours.")
            return
        for p in targets:
            print(p)
        return

    pipeline_filter: str = getattr(args, "pipeline", None)
    items = list(pulses.values())
    if pipeline_filter:
        items = [p for p in items if p.pipeline == pipeline_filter]

    if not items:
        print("No matching pipelines found.")
        return

    for result in items:
        print(result)


def register_pulse_subcommands(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser: argparse.ArgumentParser = subparsers.add_parser(
        "pulse", help="Show pipeline activity heartbeat"
    )
    parser.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    parser.add_argument(
        "--silent-only",
        action="store_true",
        dest="silent_only",
        help="Only show pipelines with no activity in the last 24 hours",
    )
    parser.set_defaults(func=cmd_pulse)
