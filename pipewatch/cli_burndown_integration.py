"""Wire burndown subcommand into the main CLI."""
from pipewatch.cli_burndown import register_burndown_subcommands


def attach(subparsers) -> None:  # type: ignore[type-arg]
    register_burndown_subcommands(subparsers)
