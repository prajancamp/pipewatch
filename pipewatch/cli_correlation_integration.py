"""Attach correlation subcommand to main CLI."""
from pipewatch.cli_correlation import register_correlation_subcommands


def attach(sub):
    register_correlation_subcommands(sub)
