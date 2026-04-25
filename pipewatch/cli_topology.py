"""CLI subcommand: pipewatch topology — show pipeline influence ranking."""
from __future__ import annotations

import argparse

from pipewatch.pipeline_map import PipelineMap, load_pipeline_map
from pipewatch.topology import analyze_topology


def cmd_topology(args: argparse.Namespace) -> None:
    pipeline_map: PipelineMap = load_pipeline_map(args.store)

    if not pipeline_map.nodes:
        print("No pipeline map found. Use 'pipewatch map add' to define edges.")
        return

    results = analyze_topology(pipeline_map, hub_threshold=args.hub_threshold)

    if args.pipeline:
        results = [r for r in results if r.pipeline == args.pipeline]

    if not results:
        print("No results.")
        return

    header = f"{'Pipeline':<30} {'Upstream':>8} {'Downstream':>10} {'Influence':>9} {'Hub':>5}"
    print(header)
    print("-" * len(header))
    for r in results:
        hub_flag = "yes" if r.is_hub else ""
        print(
            f"{r.pipeline:<30} {r.upstream_count:>8} {r.downstream_count:>10} "
            f"{r.influence_score:>9.1f} {hub_flag:>5}"
        )


def register_topology_subcommands(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser(
        "topology",
        help="Rank pipelines by influence (downstream reach + upstream exposure).",
    )
    p.add_argument("--store", default="pipewatch_data", help="Path to data store.")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline.")
    p.add_argument(
        "--hub-threshold",
        type=int,
        default=3,
        dest="hub_threshold",
        help="Minimum transitive downstream count to label a pipeline as a hub (default: 3).",
    )
    p.set_defaults(func=cmd_topology)
