"""Attach trace subcommands to the main CLI."""
from pipewatch.cli_trace import register_trace_subcommands


def attach(subparsers):
    register_trace_subcommands(subparsers)
