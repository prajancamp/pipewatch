"""CLI subcommand for detecting pipeline drift."""

from __future__ import annotations

import argparse
import sys

from pipewatch.drift import detect_all_drift, detect_drift
from pipewatch.store import RunStore


def cmd_drift(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs found.")
        sys.exit(0)

    window = getattr(args, "window", 10)
    threshold = getattr(args, "threshold", 20.0)
    pipeline = getattr(args, "pipeline", None)

    if pipeline:
        results = detect_drift(runs, pipeline, window_size=window, threshold_pct=threshold)
    else:
        results = detect_all_drift(runs, window_size=window, threshold_pct=threshold)

    if not results:
        print("No drift detected (insufficient data or no significant changes).")
        sys.exit(0)

    flagged = [r for r in results if r.flagged]
    clean = [r for r in results if not r.flagged]

    if flagged:
        print(f"\n{'='*50}")
        print(f"  DRIFT DETECTED ({len(flagged)} metric(s) flagged)")
        print(f"{'='*50}")
        for r in flagged:
            print(f"  {r}")

    if clean:
        print(f"\n--- Stable metrics ({len(clean)}) ---")
        for r in clean:
            print(f"  {r}")

    if flagged:
        sys.exit(2)


def register_drift_subcommands(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("drift", help="Detect behavioral drift in pipelines")
    p.add_argument("--pipeline", default=None, help="Limit to a specific pipeline")
    p.add_argument(
        "--window",
        type=int,
        default=10,
        help="Number of runs in each comparison window (default: 10)",
    )
    p.add_argument(
        "--threshold",
        type=float,
        default=20.0,
        help="Percent change to flag as drift (default: 20.0)",
    )
    p.set_defaults(func=cmd_drift)
