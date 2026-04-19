"""CLI subcommands for replaying alert rules against historical data."""
from __future__ import annotations
import argparse
from pipewatch.store import RunStore
from pipewatch.alert import AlertRule
from pipewatch.replay import replay_all


def cmd_replay(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()
    if not runs:
        print("No runs found in store.")
        return

    rules: list[AlertRule] = []
    if args.min_success_rate is not None:
        rules.append(AlertRule(min_success_rate=args.min_success_rate))
    if args.max_consecutive_failures is not None:
        rules.append(AlertRule(max_consecutive_failures=args.max_consecutive_failures))
    if args.max_avg_duration is not None:
        rules.append(AlertRule(max_avg_duration=args.max_avg_duration))

    if not rules:
        print("No rules specified. Use --min-success-rate, --max-consecutive-failures, or --max-avg-duration.")
        return

    pipelines = args.pipeline or None
    results = replay_all(runs, rules, pipelines=pipelines)

    for result in results:
        print(result)


def register_replay_subcommands(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("replay", help="Replay alert rules against stored runs")
    p.add_argument("--store", required=True, help="Path to run store file")
    p.add_argument("--pipeline", nargs="+", help="Limit replay to specific pipelines")
    p.add_argument("--min-success-rate", type=float, dest="min_success_rate", default=None)
    p.add_argument("--max-consecutive-failures", type=int, dest="max_consecutive_failures", default=None)
    p.add_argument("--max-avg-duration", type=float, dest="max_avg_duration", default=None)
    p.set_defaults(func=cmd_replay)
