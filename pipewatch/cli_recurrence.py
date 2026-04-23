"""CLI sub-command: pipewatch recurrence"""
from __future__ import annotations

import argparse

from pipewatch.recurrence import detect_recurrence
from pipewatch.store import RunStore


def cmd_recurrence(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No runs in store.")
        return

    results = detect_recurrence(
        runs,
        min_occurrences=args.min_occurrences,
        min_failure_rate=args.min_failure_rate,
        pipeline=getattr(args, "pipeline", None),
    )

    if not results:
        print("No recurrent failure slots detected.")
        return

    print(f"{'Pipeline':<30} {'Slot':>6} {'Fails':>6} {'Total':>6} {'Rate':>7}")
    print("-" * 60)
    for r in results:
        print(
            f"{r.pipeline:<30} {r.hour_slot:>5}h "
            f"{r.failure_count:>6} {r.total_in_slot:>6} "
            f"{r.failure_rate:>6.0%}"
        )


def register_recurrence_subcommands(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser(
        "recurrence",
        help="Detect pipelines that fail recurrently at the same hour of day",
    )
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument(
        "--min-occurrences",
        type=int,
        default=3,
        dest="min_occurrences",
        help="Minimum runs in a slot to consider (default: 3)",
    )
    p.add_argument(
        "--min-failure-rate",
        type=float,
        default=0.5,
        dest="min_failure_rate",
        help="Minimum failure rate to flag a slot (default: 0.5)",
    )
    p.set_defaults(func=cmd_recurrence)
