"""CLI subcommand: pipewatch forecast"""

from __future__ import annotations
import argparse
from pipewatch.store import RunStore
from pipewatch.forecast import forecast_all, forecast_pipeline


def cmd_forecast(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No runs found.")
        return

    if args.pipeline:
        result = forecast_pipeline(runs, args.pipeline, window=args.window)
        if result is None:
            print(f"Not enough data to forecast '{args.pipeline}'.")
        else:
            print(result)
        return

    results = forecast_all(runs, window=args.window)
    if not results:
        print("Not enough data to produce forecasts.")
        return

    for r in results:
        print(r)


def register_forecast_subcommands(
    subparsers: argparse._SubParsersAction,
) -> None:
    p = subparsers.add_parser(
        "forecast",
        help="Forecast pipeline failure rates based on recent trends",
    )
    p.add_argument("--pipeline", default=None, help="Limit to a single pipeline")
    p.add_argument(
        "--window",
        type=int,
        default=20,
        help="Max runs per window used for rate calculation (default: 20)",
    )
    p.set_defaults(func=cmd_forecast)
