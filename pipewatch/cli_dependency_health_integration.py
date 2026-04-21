"""Integration shim: attach dep-health subcommand to main CLI."""
from pipewatch.cli_dependency_health import register_dependency_health_subcommands


def attach(subparsers):
    register_dependency_health_subcommands(subparsers)
