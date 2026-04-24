"""CLI subcommands for incident detection."""
from __future__ import annotations

import argparse

from pipewatch.store import RunStore
from pipewatch.incident import detect_incidents


def cmd_incident(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if args.pipeline:
        runs = [r for r in runs if r.pipeline == args.pipeline]

    incidents = detect_incidents(runs, min_length=args.min_length)

    if not incidents:
        print("No incidents detected.")
        return

    for inc in incidents:
        print(str(inc))
        if args.verbose:
            for r in inc.runs:
                err = f" — {r.error}" if r.error else ""
                print(f"  {r.started_at}  {r.run_id}{err}")
        print()


def register_incident_subcommands(
    subparsers: argparse._SubParsersAction,
) -> None:
    p = subparsers.add_parser("incident", help="Detect failure incidents")
    p.add_argument("--pipeline", default=None, help="Filter by pipeline name")
    p.add_argument(
        "--min-length",
        type=int,
        default=2,
        dest="min_length",
        help="Minimum consecutive failures to form an incident (default: 2)",
    )
    p.add_argument("-v", "--verbose", action="store_true", help="Show individual runs")
    p.set_defaults(func=cmd_incident)
