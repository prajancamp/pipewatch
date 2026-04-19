"""CLI subcommands for watchdog stale-pipeline detection."""
import argparse
from pipewatch.store import RunStore
from pipewatch.watchdog import find_stale_pipelines


def cmd_watchdog(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    thresholds = {}
    if args.threshold_override:
        for item in args.threshold_override:
            try:
                name, val = item.split("=", 1)
                thresholds[name.strip()] = int(val.strip())
            except ValueError:
                print(f"[warn] Ignoring bad threshold override: {item!r}")

    alerts = find_stale_pipelines(
        runs,
        stale_after_minutes=args.stale_after,
        pipeline_thresholds=thresholds or None,
    )

    if not alerts:
        print("All pipelines are running on time.")
        return

    print(f"{'Pipeline':<30} {'Age (min)':>10} {'Threshold':>10}")
    print("-" * 54)
    for alert in sorted(alerts, key=lambda a: -a.age_minutes()):
        print(f"{alert.pipeline:<30} {alert.age_minutes():>10.1f} {alert.stale_after_minutes:>10}")


def register_watchdog_subcommands(subparsers) -> None:
    p = subparsers.add_parser("watchdog", help="Detect stale pipelines")
    p.add_argument("--store", required=True, help="Path to run store")
    p.add_argument(
        "--stale-after", type=int, default=60,
        metavar="MINUTES", help="Default stale threshold in minutes (default: 60)"
    )
    p.add_argument(
        "--threshold-override", nargs="*", metavar="PIPELINE=MINUTES",
        help="Per-pipeline threshold overrides, e.g. etl=30"
    )
    p.set_defaults(func=cmd_watchdog)
