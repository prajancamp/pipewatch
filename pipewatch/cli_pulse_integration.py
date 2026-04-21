"""Attach pulse subcommands to the main CLI."""
from pipewatch.cli_pulse import register_pulse_subcommands


def attach(subparsers) -> None:  # type: ignore[type-arg]
    register_pulse_subcommands(subparsers)
