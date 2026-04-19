"""Lint pipeline runs for common data quality issues."""
from dataclasses import dataclass, field
from typing import List
from pipewatch.models import PipelineRun


@dataclass
class LintIssue:
    run_id: str
    pipeline: str
    code: str
    message: str

    def __str__(self) -> str:
        return f"[{self.code}] {self.pipeline}/{self.run_id}: {self.message}"


@dataclass
class LintReport:
    issues: List[LintIssue] = field(default_factory=list)

    @property
    def has_issues(self) -> bool:
        return len(self.issues) > 0

    def __str__(self) -> str:
        if not self.issues:
            return "No lint issues found."
        lines = [str(i) for i in self.issues]
        return "\n".join(lines)


def lint_runs(runs: List[PipelineRun]) -> LintReport:
    issues: List[LintIssue] = []
    for run in runs:
        if not run.pipeline_name or not run.pipeline_name.strip():
            issues.append(LintIssue(run.run_id, run.pipeline_name or "", "EMPTY_NAME", "Pipeline name is empty"))
        if run.is_failed() and not run.error:
            issues.append(LintIssue(run.run_id, run.pipeline_name, "FAILED_NO_ERROR", "Failed run has no error message"))
        if run.duration is not None and run.duration < 0:
            issues.append(LintIssue(run.run_id, run.pipeline_name, "NEG_DURATION", f"Negative duration: {run.duration}"))
        if run.started_at and run.finished_at and run.finished_at < run.started_at:
            issues.append(LintIssue(run.run_id, run.pipeline_name, "TIME_INVERSION", "finished_at is before started_at"))
        if run.is_success() and run.error:
            issues.append(LintIssue(run.run_id, run.pipeline_name, "SUCCESS_WITH_ERROR", "Successful run has an error message"))
    return LintReport(issues=issues)
