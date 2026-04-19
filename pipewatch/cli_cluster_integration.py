"""Attach cluster subcommand to the main CLI."""
from pipewatch.cli_cluster import register_cluster_subcommands


def attach(subparsers) -> None:
    register_cluster_subcommands(subparsers)
