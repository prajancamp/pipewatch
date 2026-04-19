"""CLI subcommands for digest reports."""

import argparse
import json
from pathlib import Path

from pipewatch.digest import build_digest
from pipewatch.store import RunStore


def cmd_digest(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    digest = build_digest(store, period_hours=args.hours)

    if args.format == "json":
        data = {
            "generated_at": digest.generated_at,
            "period_hours": digest.period_hours,
            "pipeline_count": digest.pipeline_count,
            "total_runs": digest.total_runs,
            "failed_pipelines": digest.failed_pipelines,
        }
        print(json.dumps(data, indent=2))
    else:
        print(digest)

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w") as f:
            if args.format == "json":
                json.dump(data, f, indent=2)
            else:
                f.write(str(digest))
        print(f"Digest written to {out}")


def register_digest_subcommands(subparsers) -> None:
    p = subparsers.add_parser("digest", help="Show a periodic health digest")
    p.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Look-back window in hours (default: 24)",
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format",
    )
    p.add_argument(
        "--output",
        default=None,
        help="Optional file path to write the digest",
    )
    p.set_defaults(func=cmd_digest)
