"""CLI subcommand: pipewatch flap — detect flapping pipelines."""
from __future__ import annotations

import argparse
import sys

from pipewatch.flap import detect_flaps
from pipewatch.store import RunStore


def cmd_flap(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No runs recorded.")
        sys.exit(0)

    results = detect_flaps(
        runs,
        pipeline=getattr(args, "pipeline", None),
        min_runs=args.min_runs,
        flap_threshold=args.threshold,
    )

    if not results:
        print("No pipelines met the minimum run threshold.")
        sys.exit(0)

    flapping = [r for r in results if r.is_flapping]
    stable = [r for r in results if not r.is_flapping]

    if flapping:
        print(f"Flapping pipelines ({len(flapping)}):")
        for r in flapping:
            print(f"  {r}")
    else:
        print("No flapping pipelines detected.")

    if stable and args.verbose:
        print(f"\nStable pipelines ({len(stable)}):")
        for r in stable:
            print(f"  {r}")

    if flapping:
        sys.exit(2)


def register_flap_subcommands(sub: argparse.Action) -> None:  # type: ignore[type-arg]
    p = sub.add_parser("flap", help="Detect pipelines that oscillate between success and failure")
    p.add_argument("--store", default="pipewatch_data", help="Path to the run store directory")
    p.add_argument("--pipeline", default=None, help="Restrict analysis to a single pipeline")
    p.add_argument(
        "--min-runs",
        dest="min_runs",
        type=int,
        default=4,
        help="Minimum number of runs required (default: 4)",
    )
    p.add_argument(
        "--threshold",
        type=float,
        default=0.5,
        help="Flap rate threshold to flag a pipeline (default: 0.5)",
    )
    p.add_argument("-v", "--verbose", action="store_true", help="Also show stable pipelines")
    p.set_defaults(func=cmd_flap)
