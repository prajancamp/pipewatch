"""CLI sub-command: pipewatch signal — show pipeline signal detection results."""
from __future__ import annotations

import argparse
import sys

from pipewatch.signal import detect_signals
from pipewatch.store import RunStore


def cmd_signal(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs found.")
        sys.exit(0)

    results = detect_signals(
        runs,
        pipeline=getattr(args, "pipeline", None),
        window=args.window,
        min_runs=args.min_runs,
    )

    if not results:
        print("No signals detected (insufficient data).")
        sys.exit(0)

    # optional filter by signal type
    if args.only:
        results = [r for r in results if r.signal == args.only]

    if not results:
        print(f"No pipelines emitting '{args.only}' signal.")
        sys.exit(0)

    header = f"{'PIPELINE':<30} {'SIGNAL':<12} {'CONFIDENCE':>12}  DETAIL"
    print(header)
    print("-" * len(header))
    for r in results:
        print(r)

    # exit code 1 when any non-stable signal found (useful in CI)
    if any(r.signal != "stable" for r in results):
        sys.exit(1)


def register_signal_subcommands(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("signal", help="Detect pipeline health signals")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--window", type=int, default=10, help="Number of recent runs to analyse (default: 10)")
    p.add_argument("--min-runs", dest="min_runs", type=int, default=4, help="Minimum runs required (default: 4)")
    p.add_argument(
        "--only",
        choices=["flapping", "degrading", "recovering", "stable"],
        default=None,
        help="Show only pipelines with this signal",
    )
    p.set_defaults(func=cmd_signal)
