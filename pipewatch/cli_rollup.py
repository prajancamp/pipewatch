"""CLI subcommands for rollup reporting."""
from __future__ import annotations
import argparse
from pipewatch.store import RunStore
from pipewatch.rollup import compute_rollup


def cmd_rollup(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()
    if not runs:
        print("No runs found.")
        return

    granularity = getattr(args, "granularity", "daily")
    pipeline = getattr(args, "pipeline", None)
    if pipeline:
        runs = [r for r in runs if r.pipeline == pipeline]

    rollup = compute_rollup(runs, granularity=granularity)
    if not rollup:
        print("No data after filtering.")
        return

    for period in sorted(rollup):
        for pipe_name in sorted(rollup[period]):
            bucket = rollup[period][pipe_name]
            rate = bucket.success_rate * 100
            dur = f"{bucket.avg_duration:.1f}s" if bucket.avg_duration is not None else "n/a"
            print(
                f"{period}  {pipe_name:<30}  "
                f"runs={bucket.total}  ok={bucket.successes}  "
                f"fail={bucket.failures}  rate={rate:.0f}%  avg={dur}"
            )


def register_rollup_subcommands(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("rollup", help="Show aggregated rollup stats per period")
    p.add_argument("--granularity", choices=["daily", "hourly"], default="daily")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.set_defaults(func=cmd_rollup)
