"""Attach lint subcommands to the main CLI parser."""
from pipewatch.cli_lint import register_lint_subcommands


def attach(subparsers) -> None:
    """Register lint commands onto an existing subparsers group."""
    register_lint_subcommands(subparsers)
