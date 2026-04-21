"""CLI subcommands for cost estimation."""
from __future__ import annotations

import argparse

from pipewatch.store import RunStore
from pipewatch.cost import compute_cost_summary


def cmd_cost(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if args.pipeline:
        runs = [r for r in runs if r.pipeline == args.pipeline]

    if not runs:
        print("No runs found.")
        return

    rate = args.rate
    summaries = compute_cost_summary(runs, rate_per_second=rate)

    if not summaries:
        print("No cost data available.")
        return

    print(f"Cost Summary (rate=${rate}/s)")
    print("-" * 60)
    total_all = 0.0
    for pipeline in sorted(summaries):
        s = summaries[pipeline]
        print(str(s))
        total_all += s.total_cost_usd
    print("-" * 60)
    print(f"Grand total: ${total_all:.4f} across {len(summaries)} pipeline(s)")


def register_cost_subcommands(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    p = subparsers.add_parser("cost", help="Estimate pipeline run costs")
    p.add_argument("--pipeline", default=None, help="Filter by pipeline name")
    p.add_argument(
        "--rate",
        type=float,
        default=0.0001,
        help="Cost rate in USD per second (default: 0.0001)",
    )
    p.set_defaults(func=cmd_cost)
