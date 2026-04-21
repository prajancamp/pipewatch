"""CLI subcommands for escalation reporting."""
from __future__ import annotations

import argparse

from pipewatch.store import RunStore
from pipewatch.analyzer import compute_stats
from pipewatch.alert import AlertRule, evaluate_alerts
from pipewatch.escalation import escalate_alerts


def cmd_escalation(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No runs found.")
        return

    stats_map = compute_stats(runs)

    rules = [
        AlertRule("low_success_rate", min_success_rate=args.min_success_rate),
        AlertRule("consecutive_failures", max_consecutive_failures=args.max_consecutive),
    ]

    all_alerts = []
    for stats in stats_map.values():
        all_alerts.extend(evaluate_alerts(stats, rules))

    if not all_alerts:
        print("No active alerts — nothing to escalate.")
        return

    results = escalate_alerts(all_alerts)

    level_filter = args.level
    for result in results:
        if level_filter and result.level != level_filter:
            continue
        print(result)
        print()


def register_escalation_subcommands(
    subparsers: argparse._SubParsersAction,
) -> None:
    p = subparsers.add_parser(
        "escalation", help="Show alert escalation levels per pipeline"
    )
    p.add_argument(
        "--min-success-rate",
        type=float,
        default=0.8,
        dest="min_success_rate",
        help="Success rate threshold for alerts (default: 0.8)",
    )
    p.add_argument(
        "--max-consecutive",
        type=int,
        default=3,
        dest="max_consecutive",
        help="Max consecutive failures before alert (default: 3)",
    )
    p.add_argument(
        "--level",
        choices=["low", "medium", "high"],
        default=None,
        help="Filter output to a specific escalation level",
    )
    p.set_defaults(func=cmd_escalation)
