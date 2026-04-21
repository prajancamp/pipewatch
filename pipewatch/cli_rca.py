"""CLI subcommand: pipewatch rca — root cause analysis for failed runs."""
from __future__ import annotations

import argparse
from typing import Optional

from pipewatch.rca import analyze_all
from pipewatch.store import RunStore
from pipewatch.filter import filter_runs


def cmd_rca(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    pipeline: Optional[str] = getattr(args, "pipeline", None)
    if pipeline:
        runs = filter_runs(runs, pipeline=pipeline)

    limit: int = getattr(args, "limit", 20)
    # Only analyse the most recent *limit* failed runs
    failed = [r for r in runs if r.is_failed()]
    failed = sorted(failed, key=lambda r: r.started_at or "", reverse=True)[:limit]

    findings = analyze_all(failed)

    if not findings:
        print("No failed runs found — nothing to analyse.")
        return

    for f in findings:
        print(str(f))
        if args.verbose and f.error:
            print(f"  error: {f.error}")


def register_rca_subcommands(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    p = subparsers.add_parser("rca", help="Root cause analysis for failed runs")
    p.add_argument("--pipeline", default=None, help="Restrict to a single pipeline")
    p.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of failed runs to analyse (default: 20)",
    )
    p.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print raw error message alongside each finding",
    )
    p.set_defaults(func=cmd_rca)
