"""CLI subcommands for quorum evaluation."""
from __future__ import annotations

import argparse

from pipewatch.store import RunStore
from pipewatch.quorum import check_quorum


def cmd_quorum(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs found.")
        return

    results = check_quorum(
        runs,
        pipeline=getattr(args, "pipeline", None),
        window=args.window,
        required=args.required,
    )

    if not results:
        print("No pipelines matched.")
        return

    for r in results:
        confident_tag = "[confident]" if r.confident else "[undecided]"
        status_str = r.quorum_status.value if r.quorum_status else "none"
        print(
            f"{r.pipeline:<30} status={status_str:<10} "
            f"success={r.success_votes}/{r.window} "
            f"failure={r.failure_votes}/{r.window} "
            f"required={r.required} {confident_tag}"
        )


def register_quorum_subcommands(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("quorum", help="Evaluate quorum status across recent runs")
    p.add_argument("--pipeline", default=None, help="Limit to a single pipeline")
    p.add_argument(
        "--window",
        type=int,
        default=5,
        help="Number of most-recent runs to evaluate (default: 5)",
    )
    p.add_argument(
        "--required",
        type=int,
        default=3,
        help="Votes required to reach quorum (default: 3)",
    )
    p.set_defaults(func=cmd_quorum)
