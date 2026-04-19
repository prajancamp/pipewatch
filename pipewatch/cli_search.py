"""CLI subcommands for searching pipeline runs."""
from __future__ import annotations
import argparse
from pipewatch.store import RunStore
from pipewatch.search import search_runs


def cmd_search(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()
    if not runs:
        print("No runs in store.")
        return

    fields = args.fields.split(",") if args.fields else ["pipeline_name", "error"]
    results = search_runs(runs, args.query, fields=fields, case_sensitive=args.case_sensitive)

    if not results:
        print(f"No runs matched '{args.query}'.")
        return

    print(f"Found {len(results)} run(s) matching '{args.query}':")
    for run in results:
        status_icon = "✓" if run.is_success() else "✗"
        err_preview = f" | error: {run.error[:60]}" if run.error else ""
        print(f"  [{status_icon}] {run.pipeline_name} @ {run.started_at}  run_id={run.run_id}{err_preview}")


def register_search_subcommands(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("search", help="Search pipeline runs by field content")
    p.add_argument("query", help="Text to search for")
    p.add_argument("--fields", default=None, help="Comma-separated fields to search (default: pipeline_name,error)")
    p.add_argument("--case-sensitive", action="store_true", default=False, help="Case-sensitive search")
    p.add_argument("--store", default="pipewatch_data", help="Path to store directory")
    p.set_defaults(func=cmd_search)
