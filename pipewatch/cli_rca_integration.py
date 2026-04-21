"""Wire the rca subcommand into the main CLI parser."""
from pipewatch.cli_rca import register_rca_subcommands


def attach(subparsers) -> None:  # type: ignore[type-arg]
    register_rca_subcommands(subparsers)
