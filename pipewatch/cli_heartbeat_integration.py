"""Wire heartbeat sub-commands into the main CLI parser."""
from pipewatch.cli_heartbeat import register_heartbeat_subcommands


def attach(subparsers) -> None:  # type: ignore[type-arg]
    register_heartbeat_subcommands(subparsers)
