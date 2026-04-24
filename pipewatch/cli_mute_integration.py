"""Attach mute subcommands to the main CLI parser."""
from pipewatch.cli_mute import register_mute_subcommands


def attach(sub, store_default: str) -> None:
    register_mute_subcommands(sub, store_default)
