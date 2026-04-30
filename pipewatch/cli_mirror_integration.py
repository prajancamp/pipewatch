"""Integration shim: attach mirror subcommand to the main CLI parser."""
from pipewatch.cli_mirror import register_mirror_subcommands


def attach(subparsers) -> None:
    register_mirror_subcommands(subparsers)
