"""CLI subcommand: pipewatch correlation"""
import argparse
from pipewatch.store import RunStore
from pipewatch.correlation import compute_correlations


def cmd_correlation(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No runs in store.")
        return

    results = compute_correlations(runs, window_minutes=args.window)

    if not results:
        print("No correlated failures found.")
        return

    print(f"Correlated failures (window={args.window}m):")
    print()
    for r in results:
        print(f"  {r}")


def register_correlation_subcommands(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("correlation", help="Show pipelines with correlated failures")
    p.add_argument(
        "--window",
        type=int,
        default=5,
        metavar="MINUTES",
        help="Time window in minutes to consider failures correlated (default: 5)",
    )
    p.set_defaults(func=cmd_correlation)
