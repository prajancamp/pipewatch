"""Register spike subcommand with the main CLI."""
from pipewatch.cli_spike import register_spike_subcommands


def attach(sub):  # type: ignore[type-arg]
    register_spike_subcommands(sub)
