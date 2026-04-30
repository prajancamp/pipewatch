"""CLI subcommand: pipewatch overlap — show concurrent pipeline execution windows."""
from __future__ import annotations

import argparse

from pipewatch.store import RunStore
from pipewatch.overlap import detect_overlaps


def cmd_overlap(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs found.")
        return

    results = detect_overlaps(
        runs,
        pipeline=getattr(args, "pipeline", None),
        min_overlap_seconds=getattr(args, "min_overlap", 0.0),
    )

    if not results:
        print("No overlapping pipeline executions detected.")
        return

    print(f"{'Pipeline A':<25} {'Pipeline B':<25} {'Overlap (s)':>12}")
    print("-" * 65)
    for r in results:
        print(f"{r.pipeline_a:<25} {r.pipeline_b:<25} {r.overlap_seconds:>12.1f}")

    print(f"\n{len(results)} overlap(s) found.")


def register_overlap_subcommands(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("overlap", help="Detect overlapping pipeline executions")
    p.add_argument("--pipeline", default=None, help="Filter to a specific pipeline")
    p.add_argument(
        "--min-overlap",
        type=float,
        default=0.0,
        dest="min_overlap",
        help="Minimum overlap in seconds to report (default: 0)",
    )
    p.set_defaults(func=cmd_overlap)
