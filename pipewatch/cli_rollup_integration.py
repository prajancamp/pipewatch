"""Integration helper: attach rollup commands into the main CLI parser."""
from __future__ import annotations
import argparse
from pipewatch.cli_rollup import register_rollup_subcommands


def attach(subparsers: argparse._SubParsersAction) -> None:
    """Attach rollup subcommands to an existing subparser group."""
    register_rollup_subcommands(subparsers)


if __name__ == "__main__":  # pragma: no cover
    import sys
    from pipewatch.store import RunStore

    parser = argparse.ArgumentParser(prog="pipewatch-rollup")
    parser.add_argument("--store", required=True, help="Path to run store file")
    sub = parser.add_subparsers(dest="command")
    register_rollup_subcommands(sub)
    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()
        sys.exit(1)
