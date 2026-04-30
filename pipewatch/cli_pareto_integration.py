"""Attach pareto subcommand to the main CLI."""
from pipewatch.cli_pareto import register_pareto_subcommands


def attach(subparsers) -> None:  # noqa: ANN001
    register_pareto_subcommands(subparsers)
