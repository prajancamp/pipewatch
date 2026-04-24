"""Integration hook: attach lifespan subcommand to the main CLI."""
from pipewatch.cli_lifespan import register_lifespan_subcommands


def attach(subparsers) -> None:
    register_lifespan_subcommands(subparsers)
