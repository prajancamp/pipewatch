"""CLI subcommand: pipewatch aging — show long-unresolved failed runs."""
from __future__ import annotations

import argparse
import sys

from pipewatch.aging import detect_aging
from pipewatch.store import RunStore


def cmd_aging(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No runs recorded.")
        return

    pipeline = getattr(args, "pipeline", None)
    min_age = getattr(args, "min_age", 30.0)

    results = detect_aging(runs, min_age_minutes=min_age, pipeline=pipeline)

    if not results:
        print("No aging failures found.")
        return

    severity_icons = {"critical": "🔴", "warning": "🟡", "info": "🔵"}

    print(f"{'SEVERITY':<10} {'PIPELINE':<25} {'AGE (h)':<10} {'ERROR'}")
    print("-" * 72)
    for r in results:
        icon = severity_icons.get(r.severity, "")
        err = (r.error or "")[:40]
        print(f"{icon} {r.severity:<8} {r.pipeline:<25} {r.age_hours:<10.1f} {err}")

    critical = sum(1 for r in results if r.severity == "critical")
    if critical:
        sys.exit(2)


def register_aging_subcommands(
    subparsers: argparse._SubParsersAction,
) -> None:
    p = subparsers.add_parser(
        "aging",
        help="Show failed runs that have been unresolved for a long time",
    )
    p.add_argument(
        "--pipeline",
        default=None,
        help="Filter to a specific pipeline",
    )
    p.add_argument(
        "--min-age",
        type=float,
        default=30.0,
        dest="min_age",
        help="Minimum age in minutes to report (default: 30)",
    )
    p.set_defaults(func=cmd_aging)
