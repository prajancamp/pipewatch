"""CLI subcommands for SLA tracking."""
from __future__ import annotations

import argparse
import sys

from pipewatch.sla import check_sla
from pipewatch.store import RunStore


def cmd_sla(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No runs recorded.")
        return

    results = check_sla(
        runs,
        threshold=args.threshold,
        pipeline=getattr(args, "pipeline", None),
    )

    if not results:
        print("No matching pipelines found.")
        return

    breaching = [r for r in results if r.is_breaching]

    print(f"SLA threshold: {args.threshold:.0f}s\n")
    print(f"{'PIPELINE':<30} {'RUNS':>6} {'BREACHES':>9} {'BREACH %':>9} {'MAX DUR':>10}")
    print("-" * 68)
    for r in results:
        max_str = f"{r.max_duration:.1f}s" if r.max_duration is not None else "n/a"
        flag = " !!" if r.is_breaching else ""
        print(
            f"{r.pipeline:<30} {r.total_runs:>6} {r.breaches:>9} "
            f"{r.breach_rate * 100:>8.1f}% {max_str:>10}{flag}"
        )

    if breaching:
        print(f"\n{len(breaching)} pipeline(s) breaching SLA.")
        sys.exit(1)
    else:
        print("\nAll pipelines within SLA.")


def register_sla_subcommands(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("sla", help="Check SLA compliance for pipelines")
    p.add_argument(
        "--threshold",
        type=float,
        default=300.0,
        metavar="SECONDS",
        help="Maximum allowed run duration in seconds (default: 300)",
    )
    p.add_argument(
        "--pipeline",
        default=None,
        metavar="NAME",
        help="Restrict check to a single pipeline",
    )
    p.set_defaults(func=cmd_sla)
