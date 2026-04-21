"""CLI subcommand: dependency-health — show pipeline health with upstream context."""
from __future__ import annotations
import argparse
from pipewatch.store import RunStore
from pipewatch.pipeline_map import PipelineMap
from pipewatch.dependency_health import assess_all_dependency_health, assess_dependency_health


def cmd_dependency_health(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()
    pm = PipelineMap(store_path=args.store.parent / "pipeline_map.json")

    if not pm.all_pipelines():
        print("No pipeline map configured. Use 'pipewatch map add' to define dependencies.")
        return

    if args.pipeline:
        result = assess_dependency_health(args.pipeline, runs, pm)
        results = [result]
    else:
        results = assess_all_dependency_health(runs, pm)

    if not results:
        print("No pipelines found.")
        return

    any_blocked = False
    for r in results:
        icon = "🔴" if r.is_blocked else ("🟡" if r.upstream_issues else "🟢")
        print(f"{icon} {r.pipeline}  [{r.own_health.level}]")
        for b in r.blocked_by:
            print(f"    blocked by: {b}")
        for issue in r.upstream_issues:
            print(f"    upstream warn: {issue}")
        if r.is_blocked:
            any_blocked = True

    if any_blocked:
        raise SystemExit(1)


def register_dependency_health_subcommands(
    subparsers: argparse._SubParsersAction,
) -> None:
    p = subparsers.add_parser(
        "dep-health",
        help="Show pipeline health with upstream dependency context",
    )
    p.add_argument("--pipeline", default=None, help="Limit to a single pipeline")
    p.set_defaults(func=cmd_dependency_health)
