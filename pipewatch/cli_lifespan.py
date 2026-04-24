"""CLI subcommand: pipewatch lifespan"""
from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.lifespan import compute_all_lifespans, compute_lifespan
from pipewatch.store import RunStore


def cmd_lifespan(args: argparse.Namespace) -> None:
    store = RunStore(Path(args.store))
    runs = store.load_all()

    if not runs:
        print("No pipeline runs found.")
        return

    pipeline_filter: str | None = getattr(args, "pipeline", None)
    warn_after: float = getattr(args, "warn_after", 180.0)

    if pipeline_filter:
        result = compute_lifespan(runs, pipeline_filter, warn_after_days=warn_after)
        if result is None:
            print(f"No runs found for pipeline '{pipeline_filter}'.")
        else:
            print(result)
        return

    results = compute_all_lifespans(runs, warn_after_days=warn_after)
    if not results:
        print("No lifespan data available.")
        return

    for r in results:
        print(r)


def register_lifespan_subcommands(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "lifespan",
        help="Show how long each pipeline has been active",
    )
    p.add_argument(
        "--pipeline",
        default=None,
        help="Filter to a single pipeline",
    )
    p.add_argument(
        "--warn-after",
        type=float,
        default=180.0,
        dest="warn_after",
        help="Days before a pipeline is considered old (default: 180)",
    )
    p.set_defaults(func=cmd_lifespan)
