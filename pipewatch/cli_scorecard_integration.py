from pipewatch.cli_scorecard import register_scorecard_subcommands


def attach(subparsers) -> None:
    register_scorecard_subcommands(subparsers)
