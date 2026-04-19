"""CLI subcommands for trend analysis."""
import argparse
from pipewatch.store import RunStore
from pipewatch.trend import compute_all_trends


def cmd_trend(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs found.")
        return

    results = compute_all_trends(runs, window=args.window, threshold=args.threshold)

    if not results:
        print(
            f"Not enough data for trend analysis "
            f"(need at least {args.window * 2} runs per pipeline)."
        )
        return

    print(f"Trend Analysis  (window={args.window}, threshold={args.threshold:.0%})")
    print("-" * 52)
    for t in results:
        print(f"  {t}")
        print(
            f"    previous {args.window}: {t.window_a_success_rate:.0%} success  "
            f"recent {args.window}: {t.window_b_success_rate:.0%} success"
        )


def register_trend_subcommands(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("trend", help="Show pipeline health trends")
    p.add_argument(
        "--store",
        default="runs.jsonl",
        help="Path to run store file",
    )
    p.add_argument(
        "--window",
        type=int,
        default=5,
        help="Number of runs per comparison window (default: 5)",
    )
    p.add_argument(
        "--threshold",
        type=float,
        default=0.1,
        help="Min delta to classify as improving/degrading (default: 0.1)",
    )
    p.set_defaults(func=cmd_trend)
