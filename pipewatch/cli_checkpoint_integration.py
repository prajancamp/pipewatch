"""Attach checkpoint subcommands to the main CLI."""
from pipewatch.cli_checkpoint import register_checkpoint_subcommands


def attach(sub):
    register_checkpoint_subcommands(sub)
