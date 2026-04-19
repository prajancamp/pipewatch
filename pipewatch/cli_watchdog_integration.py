"""Attach watchdog subcommands to the main CLI."""
from pipewatch.cli_watchdog import register_watchdog_subcommands


def attach(subparsers) -> None:
    register_watchdog_subcommands(subparsers)
