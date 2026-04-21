"""CLI subcommand: pipewatch capacity — show projected run volume and compute usage."""
from __future__ import annotations

import argparse

from pipewatch.store import RunStore
from pipewatch.capacity import estimate_all_capacity, estimate_capacity


def cmd_capacity(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs recorded.")
        return

    window = getattr(args, "window", 24)

    if getattr(args, "pipeline", None):
        result = estimate_capacity(runs, args.pipeline, window_hours=window)
        if result is None:
            print(f"No runs found for '{args.pipeline}' in the last {window}h.")
        else:
            print(result)
        return

    results = estimate_all_capacity(runs, window_hours=window)
    if not results:
        print(f"No runs found in the last {window}h.")
        return

    print(f"Capacity estimate (last {window}h window)")
    print("-" * 60)
    for r in results:
        print(r)


def register_capacity_subcommands(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "capacity",
        help="Estimate projected run volume and compute usage per pipeline",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        help="Limit output to a single pipeline",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=24,
        help="Look-back window in hours (default: 24)",
    )
    parser.set_defaults(func=cmd_capacity)
