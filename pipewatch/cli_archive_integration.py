"""Attach archive subcommands to the main CLI."""

from __future__ import annotations

from pipewatch.cli_archive import register_archive_subcommands


def attach(sub) -> None:  # type: ignore[type-arg]
    register_archive_subcommands(sub)
