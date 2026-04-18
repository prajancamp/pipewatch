"""Format and print pipeline health reports to the terminal."""
from typing import Dict, List

from pipewatch.analyzer import PipelineStats

SEP = "-" * 52


def _status_icon(last_status: str) -> str:
    icons = {"success": "✓", "failed": "✗", "running": "~", "skipped": "-"}
    return icons.get(last_status, "?")


def format_stats_table(stats: Dict[str, PipelineStats]) -> str:
    """Return a formatted table string of pipeline stats."""
    if not stats:
        return "No pipeline runs recorded.\n"

    lines = [
        SEP,
        f"{'Pipeline':<24} {'Runs':>5} {'Fail%':>6} {'AvgDur(s)':>10} {'Last':>6}",
        SEP,
    ]
    for name, s in sorted(stats.items()):
        avg = f"{s.avg_duration_seconds:.1f}" if s.avg_duration_seconds is not None else "  n/a"
        icon = _status_icon(s.last_status or "")
        lines.append(
            f"{name:<24} {s.total_runs:>5} {s.failure_rate:>6.1%} {avg:>10} {icon:>6}"
        )
    lines.append(SEP)
    return "\n".join(lines) + "\n"


def format_alert_block(flagged_pipelines: List[str], threshold: int) -> str:
    """Return a warning block for pipelines with consecutive failures."""
    if not flagged_pipelines:
        return ""
    lines = [
        "",
        f"⚠  ALERT: {len(flagged_pipelines)} pipeline(s) have {threshold}+ consecutive failures:",
    ]
    for name in sorted(flagged_pipelines):
        lines.append(f"   • {name}")
    lines.append("")
    return "\n".join(lines) + "\n"


def print_report(
    stats: Dict[str, PipelineStats],
    flagged_pipelines: List[str],
    threshold: int = 3,
) -> None:
    """Print a full health report to stdout."""
    print(format_stats_table(stats))
    alert = format_alert_block(flagged_pipelines, threshold)
    if alert:
        print(alert)
