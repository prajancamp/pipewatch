"""CLI subcommand for linting pipeline run data."""
import argparse
from pipewatch.store import RunStore
from pipewatch.lint import lint_runs


def cmd_lint(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()
    if args.pipeline:
        runs = [r for r in runs if args.pipeline.lower() in r.pipeline_name.lower()]
    report = lint_runs(runs)
    print(str(report))
    if report.has_issues:
        codes = {i.code for i in report.issues}
        print(f"\n{len(report.issues)} issue(s) found. Codes: {', '.join(sorted(codes))}")
        raise SystemExit(1)


def register_lint_subcommands(subparsers) -> None:
    p = subparsers.add_parser("lint", help="Lint pipeline run records for data quality issues")
    p.add_argument("--store", required=True, help="Path to the run store directory")
    p.add_argument("--pipeline", default=None, help="Filter by pipeline name substring")
    p.set_defaults(func=cmd_lint)
