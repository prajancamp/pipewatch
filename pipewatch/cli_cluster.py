"""CLI subcommand: pipewatch cluster — show error clusters."""
from __future__ import annotations
import argparse
from pipewatch.store import RunStore
from pipewatch.cluster import cluster_by_error


def cmd_cluster(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    if args.pipeline:
        runs = [r for r in runs if r.pipeline == args.pipeline]

    clusters = cluster_by_error(runs)

    if not clusters:
        print("No failure clusters found.")
        return

    print(f"{'COUNT':<8} {'PIPELINES':<30} ERROR KEY")
    print("-" * 80)
    for c in clusters:
        pipelines = ", ".join(c.pipelines)[:28]
        key_preview = c.key[:50]
        print(f"{c.count:<8} {pipelines:<30} {key_preview}")


def register_cluster_subcommands(subparsers) -> None:
    p = subparsers.add_parser("cluster", help="Cluster runs by error similarity")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.set_defaults(func=cmd_cluster)
