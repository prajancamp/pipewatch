"""CLI subcommands for badge generation."""
from __future__ import annotations

import argparse

from pipewatch.store import RunStore
from pipewatch.badge import generate_badge, generate_all_badges


def cmd_badge(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if args.pipeline:
        badges = [generate_badge(args.pipeline, runs)]
    else:
        badges = generate_all_badges(runs)

    if not badges:
        print("No pipeline data found.")
        return

    for badge in badges:
        if args.url:
            print(f"{badge.pipeline}: {badge.to_shields_url()}")
        else:
            print(badge)


def register_badge_subcommands(
    subparsers: argparse._SubParsersAction,
) -> None:
    p = subparsers.add_parser(
        "badge", help="Generate health badges for pipelines"
    )
    p.add_argument(
        "--pipeline", default=None, help="Limit to a specific pipeline"
    )
    p.add_argument(
        "--url",
        action="store_true",
        default=False,
        help="Output shields.io badge URLs instead of plain text",
    )
    p.set_defaults(func=cmd_badge)
