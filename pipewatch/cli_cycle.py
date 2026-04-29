"""CLI subcommand: pipewatch cycle – detect repeating failure/success cycles."""
from __future__ import annotations

import argparse

from pipewatch.store import RunStore
from pipewatch.cycle import detect_cycles


def cmd_cycle(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs found.")
        return

    results = detect_cycles(
        runs,
        pipeline=getattr(args, "pipeline", None),
        min_runs=args.min_runs,
        min_confidence=args.min_confidence,
        max_period=args.max_period,
    )

    if not results:
        print("No cyclic patterns detected.")
        return

    print(f"{'Pipeline':<30} {'Period':>6} {'Confidence':>11} {'Sample':>7}  Pattern")
    print("-" * 72)
    for r in results:
        pattern_str = " → ".join(r.pattern)
        print(
            f"{r.pipeline:<30} {r.period:>6} {r.confidence:>10.0%} "
            f"{r.sample_size:>7}  {pattern_str}"
        )


def register_cycle_subcommands(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("cycle", help="Detect repeating outcome cycles")
    p.add_argument("--pipeline", default=None, help="Limit to one pipeline")
    p.add_argument("--min-runs", type=int, default=10, dest="min_runs",
                   help="Minimum runs required (default: 10)")
    p.add_argument("--min-confidence", type=float, default=0.75,
                   dest="min_confidence",
                   help="Minimum confidence threshold (default: 0.75)")
    p.add_argument("--max-period", type=int, default=8, dest="max_period",
                   help="Maximum cycle period to test (default: 8)")
    p.set_defaults(func=cmd_cycle)
