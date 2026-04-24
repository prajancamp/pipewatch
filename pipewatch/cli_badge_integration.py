"""Attach badge subcommands to the main CLI parser."""
from pipewatch.cli_badge import register_badge_subcommands


def attach(subparsers):
    register_badge_subcommands(subparsers)
