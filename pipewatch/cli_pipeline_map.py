"""CLI subcommands for managing the pipeline dependency map."""
from __future__ import annotations

import argparse
from pipewatch.pipeline_map import load_map, save_map


def cmd_map_add(args: argparse.Namespace) -> None:
    pm = load_map(args.store)
    pm.add_edge(args.upstream, args.downstream)
    save_map(args.store, pm)
    print(f"Edge added: {args.upstream} -> {args.downstream}")


def cmd_map_show(args: argparse.Namespace) -> None:
    pm = load_map(args.store)
    if not pm.nodes:
        print("No pipeline map defined.")
        return
    for name in pm.all_pipelines():
        node = pm.nodes[name]
        up = ", ".join(node.upstream) or "(none)"
        down = ", ".join(node.downstream) or "(none)"
        print(f"  {name}")
        print(f"    upstream:   {up}")
        print(f"    downstream: {down}")


def cmd_map_deps(args: argparse.Namespace) -> None:
    pm = load_map(args.store)
    name = args.pipeline
    up = pm.get_upstream(name)
    down = pm.get_downstream(name)
    print(f"Pipeline: {name}")
    print(f"  upstream:   {', '.join(up) if up else '(none)'}")
    print(f"  downstream: {', '.join(down) if down else '(none)'}")


def register_pipeline_map_subcommands(sub: argparse._SubParsersAction) -> None:
    p_map = sub.add_parser("map", help="Manage pipeline dependency map")
    map_sub = p_map.add_subparsers(dest="map_cmd")

    add_p = map_sub.add_parser("add", help="Add a dependency edge")
    add_p.add_argument("upstream", help="Upstream pipeline name")
    add_p.add_argument("downstream", help="Downstream pipeline name")
    add_p.set_defaults(func=cmd_map_add)

    show_p = map_sub.add_parser("show", help="Show full pipeline map")
    show_p.set_defaults(func=cmd_map_show)

    deps_p = map_sub.add_parser("deps", help="Show deps for a single pipeline")
    deps_p.add_argument("pipeline", help="Pipeline name")
    deps_p.set_defaults(func=cmd_map_deps)
