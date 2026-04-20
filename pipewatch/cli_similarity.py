"""CLI subcommand: pipewatch similarity — show similar pipeline failure profiles."""
from __future__ import annotations

import argparse

from pipewatch.store import RunStore
from pipewatch.similarity import find_similar_pipelines


def cmd_similarity(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No runs found.")
        return

    threshold = args.threshold
    results = find_similar_pipelines(runs, threshold=threshold)

    if not results:
        print(f"No pipeline pairs found with similarity >= {threshold:.2f}.")
        return

    print(f"Pipeline similarity (threshold={threshold:.2f}):\n")
    print(f"  {'Pipeline A':<30} {'Pipeline B':<30} {'Score':>6}  {'Shared Err Tokens':>17}  {'Rate Delta':>10}")
    print("  " + "-" * 82)
    for r in results:
        print(
            f"  {r.pipeline_a:<30} {r.pipeline_b:<30} {r.score:>6.2f}"
            f"  {r.shared_errors:>17}  {r.failure_rate_delta:>10.2f}"
        )


def register_similarity_subcommands(
    subparsers: argparse._SubParsersAction,
) -> None:
    p = subparsers.add_parser(
        "similarity",
        help="Show pipelines with similar failure profiles",
    )
    p.add_argument(
        "--threshold",
        type=float,
        default=0.6,
        help="Minimum similarity score to display (default: 0.6)",
    )
    p.set_defaults(func=cmd_similarity)
