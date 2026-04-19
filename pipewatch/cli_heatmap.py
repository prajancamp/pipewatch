"""CLI subcommand: pipewatch heatmap"""
from __future__ import annotations
import argparse
from pipewatch.store import RunStore
from pipewatch.heatmap import compute_heatmap, top_failure_slots


def cmd_heatmap(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No runs found.")
        return

    pipeline = getattr(args, "pipeline", None)
    cells = compute_heatmap(runs, pipeline=pipeline)

    if not cells:
        print("No heatmap data available.")
        return

    if getattr(args, "top", False):
        cells = top_failure_slots(cells, n=args.top)
        print(f"Top {args.top} failure slots:")
    else:
        print("Failure heatmap (day x hour):")

    print(f"{'Day':<5} {'Hour':>4}  {'Runs':>6}  {'Failures':>8}  {'Rate':>6}")
    print("-" * 40)
    for cell in cells:
        bar = "#" * int(cell.failure_rate * 20)
        print(f"{cell.day:<5} {cell.hour:>4}  {cell.total:>6}  {cell.failures:>8}  {cell.failure_rate:>5.0%}  {bar}")


def register_heatmap_subcommands(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("heatmap", help="Show failure heatmap by day and hour")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--top", type=int, default=0, help="Show only top N failure slots")
    p.set_defaults(func=cmd_heatmap)
