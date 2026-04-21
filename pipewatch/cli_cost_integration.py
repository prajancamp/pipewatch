"""Attach cost subcommands to the main CLI parser."""
from pipewatch.cli_cost import register_cost_subcommands


def attach(subparsers):  # type: ignore[no-untyped-def]
    register_cost_subcommands(subparsers)
