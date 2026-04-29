"""CLI subcommand: pipewatch succession"""
from __future__ import annotations

import argparse

from pipewatch.store import RunStore
from pipewatch.succession import detect_succession


def cmd_succession(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs found.")
        return

    results = detect_succession(
        runs,
        window_seconds=args.window,
        min_rate=args.min_rate,
        min_occurrences=args.min_occurrences,
        pipeline=args.pipeline or None,
    )

    if not results:
        print("No succession patterns detected.")
        return

    print(f"{'TRIGGER':<30} {'SUCCESSOR':<30} {'TRIGGERS':>8} {'HITS':>5} {'RATE':>7}")
    print("-" * 82)
    for r in results:
        print(
            f"{r.trigger_pipeline:<30} {r.successor_pipeline:<30} "
            f"{r.trigger_failures:>8} {r.successor_failures_after:>5} "
            f"{r.rate:>6.0%}"
        )


def register_succession_subcommands(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    p = subparsers.add_parser(
        "succession",
        help="Detect pipelines that consistently fail after another pipeline fails",
    )
    p.add_argument("--store", default="pipewatch_data", help="Path to run store")
    p.add_argument(
        "--window",
        type=float,
        default=300.0,
        metavar="SECONDS",
        help="Time window after a trigger failure to look for successor failures (default: 300)",
    )
    p.add_argument(
        "--min-rate",
        type=float,
        default=0.5,
        dest="min_rate",
        metavar="RATE",
        help="Minimum co-occurrence rate to surface a result (default: 0.5)",
    )
    p.add_argument(
        "--min-occurrences",
        type=int,
        default=2,
        dest="min_occurrences",
        metavar="N",
        help="Minimum absolute co-occurrence count (default: 2)",
    )
    p.add_argument(
        "--pipeline",
        default="",
        help="Restrict analysis to runs where this pipeline is the trigger",
    )
    p.set_defaults(func=cmd_succession)
