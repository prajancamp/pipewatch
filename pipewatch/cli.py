"""CLI entry point for pipewatch."""
import argparse
import sys
from datetime import datetime, timezone

from pipewatch.store import RunStore
from pipewatch.analyzer import compute_stats, find_consecutive_failures
from pipewatch.report import print_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Lightweight CLI monitor for ETL pipeline health.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # report command
    report_parser = subparsers.add_parser("report", help="Print pipeline health report")
    report_parser.add_argument("--store", default="runs.jsonl", help="Path to run store file")
    report_parser.add_argument("--pipeline", default=None, help="Filter by pipeline name")
    report_parser.add_argument(
        "--consecutive-threshold",
        type=int,
        default=3,
        help="Alert threshold for consecutive failures (default: 3)",
    )

    # ingest command
    ingest_parser = subparsers.add_parser("ingest", help="Record a pipeline run")
    ingest_parser.add_argument("pipeline", help="Pipeline name")
    ingest_parser.add_argument("status", choices=["success", "failed", "running", "unknown"])
    ingest_parser.add_argument("--started", default=None, help="ISO start time (default: now)")
    ingest_parser.add_argument("--ended", default=None, help="ISO end time")
    ingest_parser.add_argument("--store", default="runs.jsonl", help="Path to run store file")
    ingest_parser.add_argument("--error", default=None, help="Error message if failed")

    return parser


def cmd_report(args: argparse.Namespace) -> int:
    store = RunStore(args.store)
    if args.pipeline:
        runs = store.load_by_pipeline(args.pipeline)
    else:
        runs = store.load_all()

    stats = compute_stats(runs)
    alerts = find_consecutive_failures(runs, threshold=args.consecutive_threshold)
    print_report(stats, alerts)
    return 0


def cmd_ingest(args: argparse.Namespace) -> int:
    from pipewatch.models import PipelineRun, PipelineStatus

    started = datetime.fromisoformat(args.started) if args.started else datetime.now(timezone.utc)
    ended = datetime.fromisoformat(args.ended) if args.ended else None
    status = PipelineStatus(args.status)

    run = PipelineRun(
        pipeline_name=args.pipeline,
        status=status,
        started_at=started,
        ended_at=ended,
        error_message=args.error,
    )
    store = RunStore(args.store)
    store.append(run)
    print(f"Recorded run for '{args.pipeline}' [{status.value}]")
    return 0


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    dispatch = {"report": cmd_report, "ingest": cmd_ingest}
    sys.exit(dispatch[args.command](args))


if __name__ == "__main__":
    main()
