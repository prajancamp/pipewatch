import argparse
from pipewatch.store import RunStore
from pipewatch.analyzer import compute_stats
from pipewatch.scorecard import build_scorecard


def cmd_scorecard(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No runs in store.")
        return

    stats_map = compute_stats(runs)
    entries = build_scorecard(list(stats_map.values()))

    if not entries:
        print("No pipelines to score.")
        return

    print(f"{'Pipeline':<30} {'Grade':<6} {'Score':>6}  Notes")
    print("-" * 72)
    for entry in entries:
        notes = "; ".join(entry.reasons) if entry.reasons else "OK"
        print(f"{entry.pipeline:<30} {entry.grade:<6} {entry.score:>6.1f}  {notes}")


def register_scorecard_subcommands(subparsers) -> None:
    p = subparsers.add_parser("scorecard", help="Show pipeline health scorecards")
    p.set_defaults(func=cmd_scorecard)
