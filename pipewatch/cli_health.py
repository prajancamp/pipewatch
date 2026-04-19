import argparse
from pipewatch.store import RunStore
from pipewatch.health import assess_health, overall_level


def cmd_health(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if not runs:
        print("No pipeline runs found.")
        return

    statuses = assess_health(
        runs,
        warn_threshold=args.warn_rate,
        critical_threshold=args.critical_rate,
        consecutive_fail_warn=args.warn_consec,
        consecutive_fail_critical=args.critical_consec,
    )

    for s in statuses:
        print(s)

    overall = overall_level(statuses)
    icons = {"ok": "✅", "warn": "⚠️", "critical": "🔴"}
    print(f"\nOverall: {icons[overall]} {overall.upper()}")

    if args.exit_code:
        if overall == "critical":
            raise SystemExit(2)
        if overall == "warn":
            raise SystemExit(1)


def register_health_subcommands(subparsers) -> None:
    p = subparsers.add_parser("health", help="Show pipeline health overview")
    p.add_argument("--store", required=True, help="Path to run store file")
    p.add_argument(
        "--warn-rate", type=float, default=0.8,
        help="Success rate below which a pipeline is warned (default: 0.8)"
    )
    p.add_argument(
        "--critical-rate", type=float, default=0.5,
        help="Success rate below which a pipeline is critical (default: 0.5)"
    )
    p.add_argument(
        "--warn-consec", type=int, default=2,
        help="Consecutive failures for warn level (default: 2)"
    )
    p.add_argument(
        "--critical-consec", type=int, default=4,
        help="Consecutive failures for critical level (default: 4)"
    )
    p.add_argument(
        "--exit-code", action="store_true",
        help="Exit with code 1 (warn) or 2 (critical) based on health"
    )
    p.set_defaults(func=cmd_health)
