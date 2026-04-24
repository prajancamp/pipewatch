"""CLI subcommands for watermark management."""
from __future__ import annotations

import argparse
from pathlib import Path

from pipewatch.store import RunStore
from pipewatch.watermark import load_watermarks, update_watermarks


def cmd_watermark_update(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()
    if not runs:
        print("No runs found — nothing to update.")
        return
    updated = update_watermarks(Path(args.store), runs)
    print(f"Updated {len(updated)} watermark entries.")
    for entry in updated:
        print(f"  {entry}")


def cmd_watermark_show(args: argparse.Namespace) -> None:
    entries = load_watermarks(Path(args.store))
    if not entries:
        print("No watermarks recorded yet.")
        return

    pipeline_filter: str | None = getattr(args, "pipeline", None)
    metric_filter: str | None = getattr(args, "metric", None)

    shown = [
        e for e in entries
        if (pipeline_filter is None or e.pipeline == pipeline_filter)
        and (metric_filter is None or e.metric == metric_filter)
    ]

    if not shown:
        print("No watermarks match the given filters.")
        return

    for entry in shown:
        print(f"  {entry}")


def register_watermark_subcommands(
    subparsers: argparse._SubParsersAction,  # type: ignore[type-arg]
) -> None:
    p_update = subparsers.add_parser(
        "watermark-update", help="Recompute and persist high-water marks"
    )
    p_update.set_defaults(func=cmd_watermark_update)

    p_show = subparsers.add_parser(
        "watermark-show", help="Display stored high-water marks"
    )
    p_show.add_argument("--pipeline", default=None, help="Filter by pipeline name")
    p_show.add_argument(
        "--metric",
        default=None,
        choices=["success_rate", "avg_duration", "run_count"],
        help="Filter by metric type",
    )
    p_show.set_defaults(func=cmd_watermark_show)
