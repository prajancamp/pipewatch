"""CLI subcommand: pipewatch bottleneck — show slow pipeline bottlenecks."""
from __future__ import annotations

import argparse

from pipewatch.bottleneck import detect_bottlenecks
from pipewatch.store import RunStore


def cmd_bottleneck(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No runs found.")
        return

    results = detect_bottlenecks(
        runs,
        threshold=args.threshold,
        min_runs=args.min_runs,
        pipeline=getattr(args, "pipeline", None),
    )

    if not results:
        print("No pipelines met the minimum run count for analysis.")
        return

    bottlenecks = [r for r in results if r.is_bottleneck]
    ok = [r for r in results if not r.is_bottleneck]

    if bottlenecks:
        print(f"=== Bottlenecks (p90 > {args.threshold:.0f}s) ===")
        for r in bottlenecks:
            print(f"  {r}")
    else:
        print(f"No bottlenecks detected (threshold={args.threshold:.0f}s).")

    if args.verbose and ok:
        print("\n=== Healthy pipelines ===")
        for r in ok:
            print(f"  {r}")


def register_bottleneck_subcommands(
    subparsers: argparse._SubParsersAction,
    store_default: str,
) -> None:
    p = subparsers.add_parser(
        "bottleneck",
        help="Identify pipelines with consistently high durations.",
    )
    p.add_argument("--store", default=store_default)
    p.add_argument(
        "--threshold",
        type=float,
        default=300.0,
        help="p90 duration threshold in seconds (default: 300).",
    )
    p.add_argument(
        "--min-runs",
        dest="min_runs",
        type=int,
        default=3,
        help="Minimum runs required to evaluate a pipeline (default: 3).",
    )
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline.")
    p.add_argument("-v", "--verbose", action="store_true", help="Show healthy pipelines too.")
    p.set_defaults(func=cmd_bottleneck)
