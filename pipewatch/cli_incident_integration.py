"""Integration hook: attach incident subcommands to the main CLI parser."""
from pipewatch.cli_incident import register_incident_subcommands


def attach(subparsers):
    register_incident_subcommands(subparsers)
