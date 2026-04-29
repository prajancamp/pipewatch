"""Attach cycle subcommand to the main CLI parser."""
from pipewatch.cli_cycle import register_cycle_subcommands


def attach(subparsers) -> None:  # type: ignore[type-arg]
    register_cycle_subcommands(subparsers)
