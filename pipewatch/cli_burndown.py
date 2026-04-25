"""CLI subcommand for burndown report."""
from __future__ import annotations

import argparse

from pipewatch.burndown import compute_burndown
from pipewatch.store import RunStore


def cmd_burndown(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs found.")
        return

    pipeline = getattr(args, "pipeline", None)
    report = compute_burndown(runs, pipeline=pipeline)

    if not report.points:
        label = pipeline or "(all pipelines)"
        print(f"No data for {label}.")
        return

    print(str(report))

    if args.points:
        print("\nTimeline:")
        for pt in report.points:
            print(f"  {pt}")


def register_burndown_subcommands(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    p = subparsers.add_parser(
        "burndown",
        help="Show failure burndown over time",
    )
    p.add_argument(
        "--pipeline",
        default=None,
        help="Filter to a specific pipeline",
    )
    p.add_argument(
        "--points",
        action="store_true",
        default=False,
        help="Print individual timeline points",
    )
    p.set_defaults(func=cmd_burndown)
