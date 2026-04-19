"""CLI subcommands for retention policy management."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.store import RunStore
from pipewatch.retention import apply_retention


def cmd_retention_prune(args: argparse.Namespace) -> None:
    store = RunStore(Path(args.store))
    max_age = getattr(args, "max_age_days", None)
    max_count = getattr(args, "max_count", None)

    if max_age is None and max_count is None:
        print("Specify --max-age-days and/or --max-count.")
        return

    removed = apply_retention(store, max_age_days=max_age, max_count=max_count)
    remaining = len(store.load_all())
    print(f"Pruned {removed} run(s). {remaining} run(s) remaining.")


def register_retention_subcommands(
    subparsers: argparse._SubParsersAction,
    default_store: str,
) -> None:
    parser = subparsers.add_parser(
        "prune",
        help="Remove old pipeline runs according to a retention policy.",
    )
    parser.add_argument(
        "--store",
        default=default_store,
        help="Path to the run store (JSONL file).",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=None,
        dest="max_age_days",
        help="Remove runs older than this many days.",
    )
    parser.add_argument(
        "--max-count",
        type=int,
        default=None,
        dest="max_count",
        help="Keep only the N most recent runs.",
    )
    parser.set_defaults(func=cmd_retention_prune)
