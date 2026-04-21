"""CLI subcommand: pipewatch rerun — suggest pipelines worth retrying."""

from __future__ import annotations

import argparse
from typing import List

from pipewatch.store import RunStore
from pipewatch.rerun import suggest_reruns
from pipewatch.filter import filter_runs


def cmd_rerun(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if args.pipeline:
        runs = filter_runs(runs, pipeline=args.pipeline)

    candidates = suggest_reruns(
        runs,
        max_consecutive=args.max_consecutive,
        min_success_rate=args.min_success_rate,
    )

    if not candidates:
        print("No rerun candidates found.")
        return

    print(f"Rerun candidates ({len(candidates)}):")
    print("-" * 60)
    for c in candidates:
        print(str(c))
        if args.verbose and c.last_error:
            print(f"  error detail: {c.last_error}")


def register_rerun_subcommands(
    subparsers: argparse._SubParsersAction,
    store_default: str,
) -> None:
    p = subparsers.add_parser(
        "rerun",
        help="Suggest pipelines that are good candidates for retry",
    )
    p.add_argument("--store", default=store_default, help="Path to run store")
    p.add_argument("--pipeline", default=None, help="Limit to a specific pipeline")
    p.add_argument(
        "--max-consecutive",
        type=int,
        default=3,
        dest="max_consecutive",
        help="Skip pipelines with more than N consecutive failures (default: 3)",
    )
    p.add_argument(
        "--min-success-rate",
        type=float,
        default=0.5,
        dest="min_success_rate",
        help="Minimum historical success rate to consider (default: 0.5)",
    )
    p.add_argument(
        "-v", "--verbose",
        action="store_true",
        default=False,
        help="Show full error detail for each candidate",
    )
    p.set_defaults(func=cmd_rerun)
