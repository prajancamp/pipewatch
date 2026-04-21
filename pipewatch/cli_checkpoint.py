"""CLI subcommands for checkpoint management."""
from __future__ import annotations

import argparse

from pipewatch.checkpoint import (
    get_checkpoint,
    load_checkpoints,
    seconds_since_checkpoint,
    update_checkpoints,
)
from pipewatch.store import RunStore


def cmd_checkpoint_update(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()
    checkpoints = update_checkpoints(args.store, runs)
    if not checkpoints:
        print("No successful runs found; no checkpoints updated.")
        return
    for entry in sorted(checkpoints.values(), key=lambda e: e.pipeline):
        print(str(entry))


def cmd_checkpoint_show(args: argparse.Namespace) -> None:
    if args.pipeline:
        entry = get_checkpoint(args.store, args.pipeline)
        if entry is None:
            print(f"No checkpoint found for pipeline: {args.pipeline}")
            return
        age = seconds_since_checkpoint(entry)
        print(str(entry))
        print(f"  Age: {age:.0f}s ago")
    else:
        checkpoints = load_checkpoints(args.store)
        if not checkpoints:
            print("No checkpoints recorded.")
            return
        for entry in sorted(checkpoints.values(), key=lambda e: e.pipeline):
            age = seconds_since_checkpoint(entry)
            print(f"{entry}  [{age:.0f}s ago]")


def register_checkpoint_subcommands(sub: argparse.Action) -> None:
    p_update = sub.add_parser("checkpoint-update", help="Rebuild checkpoints from store")
    p_update.add_argument("--store", required=True)
    p_update.set_defaults(func=cmd_checkpoint_update)

    p_show = sub.add_parser("checkpoint-show", help="Show last-success checkpoints")
    p_show.add_argument("--store", required=True)
    p_show.add_argument("--pipeline", default=None)
    p_show.set_defaults(func=cmd_checkpoint_show)
