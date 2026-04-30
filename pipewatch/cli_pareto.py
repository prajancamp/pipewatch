"""CLI subcommand: pipewatch pareto"""
from __future__ import annotations

import argparse

from pipewatch.store import RunStore
from pipewatch.pareto import compute_pareto, pareto_boundary


def cmd_pareto(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs found.")
        return

    entries = compute_pareto(
        runs,
        pipeline=getattr(args, "pipeline", None),
        threshold=args.threshold,
    )

    if not entries:
        print("No data available for Pareto analysis.")
        return

    boundary = pareto_boundary(entries, threshold=args.threshold)
    boundary_set = {e.pipeline for e in boundary}

    total_failures = sum(e.failure_count for e in entries)
    print(f"Total failures across all pipelines: {total_failures}")
    print(f"Pareto threshold: {args.threshold:.0%}\n")
    print(f"{'Pipeline':<30} {'Failures':>8} {'Rate':>7} {'Cum%':>7}  Pareto")
    print("-" * 65)
    for e in entries:
        marker = "<-- " if e.pipeline in boundary_set else ""
        print(
            f"{e.pipeline:<30} {e.failure_count:>8} "
            f"{e.failure_rate():>6.0%} "
            f"{e.cumulative_failure_pct:>6.0%}  {marker}"
        )


def register_pareto_subcommands(
    subparsers: argparse.Action,
) -> None:
    p = subparsers.add_parser(
        "pareto",
        help="Identify the pipelines responsible for the majority of failures",
    )
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument(
        "--threshold",
        type=float,
        default=0.8,
        help="Cumulative failure share to highlight (default: 0.80)",
    )
    p.set_defaults(func=cmd_pareto)
