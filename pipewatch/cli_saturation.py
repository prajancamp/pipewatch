"""CLI subcommand: pipewatch saturation."""
from __future__ import annotations

import argparse

from pipewatch.saturation import check_saturation
from pipewatch.store import RunStore


def cmd_saturation(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs recorded.")
        return

    results = check_saturation(
        runs,
        max_runs=args.max_runs,
        window_hours=args.window,
        pipeline=getattr(args, "pipeline", None),
    )

    if not results:
        print("No pipelines found.")
        return

    saturated = [r for r in results if r.is_saturated]
    if saturated:
        print(f"⚠️  {len(saturated)} saturated pipeline(s):")
        for r in saturated:
            print(f"  {r}")
    else:
        print("✅ No pipelines are saturated.")

    print()
    print("All pipelines:")
    for r in results:
        icon = "🔴" if r.is_saturated else "🟢"
        print(f"  {icon} {r}")


def register_saturation_subcommands(
    subparsers: argparse._SubParsersAction,
) -> None:
    p = subparsers.add_parser(
        "saturation",
        help="Detect pipelines running at or over run-count capacity.",
    )
    p.add_argument(
        "--max-runs",
        type=int,
        default=10,
        dest="max_runs",
        help="Maximum expected runs per window (default: 10).",
    )
    p.add_argument(
        "--window",
        type=int,
        default=1,
        help="Window size in hours (default: 1).",
    )
    p.add_argument(
        "--pipeline",
        default=None,
        help="Restrict output to a single pipeline.",
    )
    p.set_defaults(func=cmd_saturation)
