"""CLI helpers for snapshot sub-commands."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipewatch.analyzer import compute_stats
from pipewatch.snapshot import capture_snapshot, diff_snapshots, load_snapshot, save_snapshot
from pipewatch.store import RunStore


def cmd_snapshot_save(args: argparse.Namespace) -> None:
    store = RunStore(Path(args.store))
    runs = store.load_all()
    stats = list(compute_stats(runs).values())
    snap = capture_snapshot(stats)
    out = Path(args.output)
    save_snapshot(snap, out)
    print(f"Snapshot saved to {out} ({len(stats)} pipeline(s))")


def cmd_snapshot_diff(args: argparse.Namespace) -> None:
    old = load_snapshot(Path(args.old))
    new = load_snapshot(Path(args.new))
    if old is None:
        print(f"ERROR: snapshot not found: {args.old}")
        return
    if new is None:
        print(f"ERROR: snapshot not found: {args.new}")
        return
    diff = diff_snapshots(old, new)
    if not diff:
        print("No changes detected between snapshots.")
        return
    print(f"Diff ({old.captured_at} -> {new.captured_at}):")
    for pid, delta in diff.items():
        if delta.get("new"):
            print(f"  {pid}: NEW pipeline")
        else:
            parts = ", ".join(f"{k}: {v:+.4f}" for k, v in delta.items())
            print(f"  {pid}: {parts}")


def register_snapshot_subcommands(subparsers) -> None:
    p_save = subparsers.add_parser("snapshot-save", help="Save current stats as a snapshot")
    p_save.add_argument("--store", default="runs.jsonl")
    p_save.add_argument("--output", required=True, help="Path to write snapshot JSON")
    p_save.set_defaults(func=cmd_snapshot_save)

    p_diff = subparsers.add_parser("snapshot-diff", help="Diff two snapshots")
    p_diff.add_argument("--old", required=True)
    p_diff.add_argument("--new", required=True)
    p_diff.set_defaults(func=cmd_snapshot_diff)
