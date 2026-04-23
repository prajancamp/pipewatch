"""Wire recurrence sub-command into the main CLI parser."""
from pipewatch.cli_recurrence import register_recurrence_subcommands


def attach(subparsers):
    register_recurrence_subcommands(subparsers)
