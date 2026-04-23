"""CLI subcommands for pipeline attribution reporting."""
from __future__ import annotations

import argparse

from pipewatch.attribution import attribute_runs, attribution_by_team
from pipewatch.store import RunStore


def cmd_attribution(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs found.")
        return

    entries = attribute_runs(runs, pipeline=getattr(args, "pipeline", None))

    if not entries:
        print("No attribution data available.")
        return

    if getattr(args, "by_team", False):
        grouped = attribution_by_team(entries)
        for team, team_entries in sorted(grouped.items()):
            print(f"\n[team: {team}]")
            for entry in team_entries:
                print(f"  {entry}")
    else:
        print(f"{'Pipeline':<30} {'Owner':<20} {'Team':<15} {'Runs':>5} {'Fail':>5} {'OK%':>6}")
        print("-" * 85)
        for entry in entries:
            owner = entry.owner or "-"
            team = entry.team or "-"
            print(
                f"{entry.pipeline:<30} {owner:<20} {team:<15} "
                f"{entry.total_runs:>5} {entry.failed_runs:>5} "
                f"{entry.success_rate:>5.0%}"
            )


def register_attribution_subcommands(
    subparsers: argparse._SubParsersAction,
) -> None:
    p = subparsers.add_parser(
        "attribution",
        help="Show owner/team attribution for pipelines",
    )
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument(
        "--by-team",
        action="store_true",
        default=False,
        help="Group output by team",
    )
    p.set_defaults(func=cmd_attribution)
