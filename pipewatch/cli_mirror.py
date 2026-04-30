"""CLI subcommand: mirror — compare two time windows side-by-side."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pipewatch.mirror import compute_mirror
from pipewatch.store import RunStore


def cmd_mirror(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    all_runs = store.load_all()

    if not all_runs:
        print("No runs in store.")
        return

    try:
        left_start = datetime.fromisoformat(args.left_start).replace(tzinfo=timezone.utc)
        left_end = datetime.fromisoformat(args.left_end).replace(tzinfo=timezone.utc)
        right_start = datetime.fromisoformat(args.right_start).replace(tzinfo=timezone.utc)
        right_end = datetime.fromisoformat(args.right_end).replace(tzinfo=timezone.utc)
    except ValueError as exc:
        print(f"Invalid datetime: {exc}")
        return

    left_runs = [
        r for r in all_runs
        if r.started_at and left_start <= r.started_at <= left_end
    ]
    right_runs = [
        r for r in all_runs
        if r.started_at and right_start <= r.started_at <= right_end
    ]

    entries = compute_mirror(left_runs, right_runs, pipeline=getattr(args, "pipeline", None))

    if not entries:
        print("No pipelines found in the specified windows.")
        return

    print(f"{'Pipeline':<30} {'Left SR':>8} {'Right SR':>9} {'Delta':>8} {'L Runs':>7} {'R Runs':>7}")
    print("-" * 75)
    for e in entries:
        sr_l = f"{e.left_success_rate:.1%}" if e.left_success_rate is not None else "N/A"
        sr_r = f"{e.right_success_rate:.1%}" if e.right_success_rate is not None else "N/A"
        delta = e.success_rate_delta
        delta_str = f"{delta:+.1%}" if delta is not None else "N/A"
        print(f"{e.pipeline:<30} {sr_l:>8} {sr_r:>9} {delta_str:>8} {e.left_total:>7} {e.right_total:>7}")


def register_mirror_subcommands(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("mirror", help="Compare two time windows side-by-side")
    p.add_argument("--left-start", required=True, help="ISO datetime for left window start")
    p.add_argument("--left-end", required=True, help="ISO datetime for left window end")
    p.add_argument("--right-start", required=True, help="ISO datetime for right window start")
    p.add_argument("--right-end", required=True, help="ISO datetime for right window end")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.set_defaults(func=cmd_mirror)
