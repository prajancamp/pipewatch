"""CLI sub-commands for the triage feature."""
from __future__ import annotations

import argparse
from typing import List

from pipewatch.store import RunStore
from pipewatch.triage import triage_runs


def cmd_triage(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs found.")
        return

    results = triage_runs(
        runs,
        min_priority=args.min_priority,
        pipeline=getattr(args, "pipeline", None),
    )

    if not results:
        print("Nothing requires attention at the selected priority threshold.")
        return

    print(f"{'PRIORITY':<10} {'PIPELINE':<30} {'SCORE':>6}  REASONS")
    print("-" * 72)
    for r in results:
        reasons = "; ".join(r.reasons) if r.reasons else "-"
        print(f"{r.label:<10} {r.pipeline:<30} {r.score:>6.2f}  {reasons}")


def register_triage_subcommands(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    p = subparsers.add_parser("triage", help="Prioritise pipelines needing attention")
    p.add_argument(
        "--min-priority",
        type=int,
        default=1,
        choices=[0, 1, 2, 3],
        metavar="LEVEL",
        help="Minimum priority level to show (0=LOW … 3=CRITICAL, default 1)",
    )
    p.add_argument(
        "--pipeline",
        default=None,
        metavar="NAME",
        help="Restrict output to a single pipeline",
    )
    p.set_defaults(func=cmd_triage)
