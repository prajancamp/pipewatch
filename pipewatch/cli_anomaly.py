from __future__ import annotations
import argparse
from pipewatch.store import RunStore
from pipewatch.anomaly import detect_anomalies


def cmd_anomaly(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if args.pipeline:
        runs = [r for r in runs if r.pipeline == args.pipeline]

    if not runs:
        print("No runs found.")
        return

    anomalies = detect_anomalies(runs, z_threshold=args.z_threshold)

    if not anomalies:
        print("No anomalies detected.")
        return

    print(f"Detected {len(anomalies)} anomaly(ies):")
    for a in anomalies:
        avg = f"{a.avg_duration:.1f}s" if a.avg_duration is not None else "n/a"
        print(f"  {a}  (avg={avg})")


def register_anomaly_subcommands(subparsers) -> None:
    p = subparsers.add_parser("anomaly", help="Detect anomalous pipeline runs")
    p.add_argument("--pipeline", default=None, help="Filter to a specific pipeline")
    p.add_argument(
        "--z-threshold",
        type=float,
        default=2.5,
        dest="z_threshold",
        help="Z-score threshold for flagging anomalies (default: 2.5)",
    )
    p.set_defaults(func=cmd_anomaly)
