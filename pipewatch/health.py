from dataclasses import dataclass
from typing import List, Optional
from pipewatch.models import PipelineRun
from pipewatch.analyzer import compute_stats


@dataclass
class HealthStatus:
    pipeline: str
    level: str  # "ok", "warn", "critical"
    reason: str

    def __str__(self) -> str:
        icons = {"ok": "✅", "warn": "⚠️", "critical": "🔴"}
        icon = icons.get(self.level, "?")
        return f"{icon} [{self.pipeline}] {self.reason}"


def assess_health(
    runs: List[PipelineRun],
    warn_threshold: float = 0.8,
    critical_threshold: float = 0.5,
    consecutive_fail_warn: int = 2,
    consecutive_fail_critical: int = 4,
) -> List[HealthStatus]:
    """Assess health for each pipeline in the run list."""
    from pipewatch.analyzer import find_consecutive_failures

    stats_map = compute_stats(runs)
    results: List[HealthStatus] = []

    for pipeline, stats in stats_map.items():
        consec = find_consecutive_failures(
            [r for r in runs if r.pipeline == pipeline]
        )
        rate = stats.success_rate

        if consec >= consecutive_fail_critical or rate < critical_threshold:
            level = "critical"
            reason = (
                f"{consec} consecutive failures"
                if consec >= consecutive_fail_critical
                else f"success rate {rate:.0%}"
            )
        elif consec >= consecutive_fail_warn or rate < warn_threshold:
            level = "warn"
            reason = (
                f"{consec} consecutive failures"
                if consec >= consecutive_fail_warn
                else f"success rate {rate:.0%}"
            )
        else:
            level = "ok"
            reason = f"success rate {rate:.0%}"

        results.append(HealthStatus(pipeline=pipeline, level=level, reason=reason))

    results.sort(key=lambda h: ["critical", "warn", "ok"].index(h.level))
    return results


def overall_level(statuses: List[HealthStatus]) -> str:
    levels = {s.level for s in statuses}
    if "critical" in levels:
        return "critical"
    if "warn" in levels:
        return "warn"
    return "ok"
