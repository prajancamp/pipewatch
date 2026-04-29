"""Attach quorum subcommands to the main CLI parser."""
from pipewatch.cli_quorum import register_quorum_subcommands


def attach(subparsers) -> None:  # type: ignore[type-arg]
    register_quorum_subcommands(subparsers)
