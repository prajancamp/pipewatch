"""CLI subcommands for pipeline trace inspection."""
from __future__ import annotations

import argparse

from pipewatch.store import RunStore
from pipewatch.trace import build_traces, failing_traces, trace_summary


def cmd_trace(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No runs found.")
        return

    chains = build_traces(runs)
    if not chains:
        print("No runs contain trace_id metadata.")
        return

    summary = trace_summary(chains)
    print(f"Traces: {summary['total']} total, {summary['failed']} failed, {summary['healthy']} healthy")
    print()

    if getattr(args, "failed_only", False):
        display = failing_traces(chains)
    else:
        display = list(chains.values())

    if not display:
        print("No matching traces.")
        return

    for chain in sorted(display, key=lambda c: c.trace_id):
        print(str(chain))
        if getattr(args, "verbose", False):
            for run in chain.runs:
                icon = "✗" if run.is_failed() else "✓"
                dur = f"{run.duration:.1f}s" if run.duration else "n/a"
                print(f"  {icon} [{run.pipeline}] run_id={run.run_id} duration={dur}")
                if run.error:
                    print(f"      error: {run.error}")


def register_trace_subcommands(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("trace", help="Inspect pipeline execution traces")
    p.add_argument("--failed-only", action="store_true", help="Show only traces with failures")
    p.add_argument("--verbose", "-v", action="store_true", help="Show individual run details")
    p.set_defaults(func=cmd_trace)
