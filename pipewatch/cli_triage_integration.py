"""Attach triage sub-commands to the main CLI parser."""
from pipewatch.cli_triage import register_triage_subcommands


def attach(subparsers) -> None:  # type: ignore[type-arg]
    register_triage_subcommands(subparsers)
