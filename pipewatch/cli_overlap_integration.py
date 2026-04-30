"""Attach overlap subcommand to the main CLI."""
from pipewatch.cli_overlap import register_overlap_subcommands


def attach(subparsers):
    register_overlap_subcommands(subparsers)
