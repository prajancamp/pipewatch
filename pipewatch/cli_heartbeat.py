"""CLI sub-commands for heartbeat monitoring."""
from __future__ import annotations

import argparse
import sys

from pipewatch.heartbeat import check_all_heartbeats, check_heartbeat
from pipewatch.store import RunStore


def cmd_heartbeat(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs found.")
        sys.exit(0)

    interval: float = args.interval
    grace: float = args.grace

    if args.pipeline:
        results = [check_heartbeat(runs, args.pipeline, interval, grace)]
    else:
        results = check_all_heartbeats(runs, interval, grace)

    dead_count = 0
    for result in results:
        print(str(result))
        if result.is_dead:
            dead_count += 1

    if dead_count:
        print(f"\n{dead_count} pipeline(s) appear DEAD.")
        sys.exit(1)
    else:
        print("\nAll pipelines OK.")


def register_heartbeat_subcommands(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    p = subparsers.add_parser(
        "heartbeat",
        help="Detect pipelines that have stopped reporting.",
    )
    p.add_argument(
        "--pipeline",
        default=None,
        help="Check a single pipeline by name.",
    )
    p.add_argument(
        "--interval",
        type=float,
        default=60.0,
        metavar="MINUTES",
        help="Expected run interval in minutes (default: 60).",
    )
    p.add_argument(
        "--grace",
        type=float,
        default=2.0,
        metavar="FACTOR",
        help="Grace multiplier applied to --interval before declaring dead (default: 2.0).",
    )
    p.set_defaults(func=cmd_heartbeat)
