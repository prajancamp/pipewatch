"""CLI subcommands for quota checking."""
from __future__ import annotations

import argparse

from pipewatch.store import RunStore
from pipewatch.quota import check_quota, breaching_quotas


def cmd_quota(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs found.")
        return

    results = check_quota(
        runs,
        expected_max=args.max_runs,
        window_hours=args.window,
        pipeline=getattr(args, "pipeline", None),
    )

    if args.breaching_only:
        results = breaching_quotas(results)

    if not results:
        print("All pipelines within quota.")
        return

    for r in results:
        print(r)

    breach_count = sum(1 for r in results if r.breaching)
    if breach_count:
        print(f"\n{breach_count} pipeline(s) exceeding quota.")
    else:
        print("\nAll pipelines within quota.")


def register_quota_subcommands(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("quota", help="Check pipeline run quotas")
    p.add_argument("--store", required=True, help="Path to run store file")
    p.add_argument(
        "--max-runs",
        type=int,
        default=100,
        help="Maximum expected runs per pipeline in the window (default: 100)",
    )
    p.add_argument(
        "--window",
        type=int,
        default=24,
        help="Time window in hours (default: 24)",
    )
    p.add_argument(
        "--pipeline",
        default=None,
        help="Limit check to a single pipeline",
    )
    p.add_argument(
        "--breaching-only",
        action="store_true",
        help="Only show pipelines exceeding their quota",
    )
    p.set_defaults(func=cmd_quota)
