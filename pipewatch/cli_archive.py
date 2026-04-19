"""CLI subcommands for archiving old pipeline runs."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from pipewatch.archive import archive_before, list_archives, load_archive
from pipewatch.store import RunStore


def cmd_archive(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    cutoff = datetime.fromisoformat(args.before).replace(tzinfo=timezone.utc)
    dest = archive_before(store, cutoff, label=args.label)
    print(f"Archived runs before {args.before} → {dest}")


def cmd_archive_list(args: argparse.Namespace) -> None:
    store_path = Path(args.store)
    archives = list_archives(store_path)
    if not archives:
        print("No archives found.")
        return
    for path in archives:
        runs = load_archive(path)
        print(f"{path.name}  ({len(runs)} runs)")


def cmd_archive_inspect(args: argparse.Namespace) -> None:
    path = Path(args.file)
    runs = load_archive(path)
    if not runs:
        print("Archive is empty.")
        return
    for run in runs:
        status = run.status.value
        print(f"  [{status}] {run.pipeline_name}  {run.run_id}  {run.started_at.isoformat()}")


def register_archive_subcommands(sub: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p_archive = sub.add_parser("archive", help="Archive old runs")
    p_archive.add_argument("--before", required=True, help="ISO datetime cutoff")
    p_archive.add_argument("--label", default=None, help="Archive file label")
    p_archive.set_defaults(func=cmd_archive)

    p_list = sub.add_parser("archive-list", help="List archive files")
    p_list.set_defaults(func=cmd_archive_list)

    p_inspect = sub.add_parser("archive-inspect", help="Inspect an archive file")
    p_inspect.add_argument("file", help="Path to .jsonl.gz archive")
    p_inspect.set_defaults(func=cmd_archive_inspect)
