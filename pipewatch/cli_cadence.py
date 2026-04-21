"""CLI subcommands for cadence analysis."""
from __future__ import annotations

import argparse

from pipewatch.cadence import compute_all_cadences, compute_cadence
from pipewatch.store import RunStore

_STATUS_ICON = {
    "on_time": "✅",
    "overdue": "⏰",
    "too_frequent": "⚡",
    "insufficient_data": "❓",
}


def cmd_cadence(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs found.")
        return

    if args.pipeline:
        results = [compute_cadence(runs, args.pipeline)]
    else:
        results = compute_all_cadences(runs)

    if not results:
        print("No cadence data available.")
        return

    print(f"{'PIPELINE':<30} {'STATUS':<16} {'EXPECTED':>10} {'LAST GAP':>10} {'RUNS':>6}")
    print("-" * 76)
    for r in results:
        icon = _STATUS_ICON.get(r.status, "?")
        exp = f"{r.expected_interval_minutes:.1f}m" if r.expected_interval_minutes is not None else "N/A"
        gap = f"{r.actual_last_gap_minutes:.1f}m" if r.actual_last_gap_minutes is not None else "N/A"
        label = f"{icon} {r.status}"
        print(f"{r.pipeline:<30} {label:<16} {exp:>10} {gap:>10} {r.run_count:>6}")
        if args.verbose:
            print(f"  → {r.note}")


def register_cadence_subcommands(
    subparsers: argparse._SubParsersAction,
) -> None:
    p = subparsers.add_parser(
        "cadence",
        help="Analyse pipeline scheduling cadence (overdue / too-frequent detection).",
    )
    p.add_argument("--pipeline", metavar="NAME", help="Limit to a single pipeline.")
    p.add_argument(
        "--verbose", "-v", action="store_true", help="Show explanatory notes."
    )
    p.set_defaults(func=cmd_cadence)
