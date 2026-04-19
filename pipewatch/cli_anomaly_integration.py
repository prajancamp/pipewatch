from pipewatch.cli_anomaly import register_anomaly_subcommands


def attach(subparsers) -> None:
    register_anomaly_subcommands(subparsers)
