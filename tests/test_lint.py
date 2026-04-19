"""Tests for pipewatch.lint."""
import pytest
from datetime import datetime, timezone
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.lint import lint_runs, LintIssue


def make_run(**kwargs) -> PipelineRun:
    defaults = dict(
        run_id="r1",
        pipeline_name="etl",
        status=PipelineStatus.SUCCESS,
        started_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
        finished_at=datetime(2024, 1, 1, 10, 5, tzinfo=timezone.utc),
        error=None,
        tags=[],
        meta={},
    )
    defaults.update(kwargs)
    return PipelineRun(**defaults)


def test_no_issues_for_clean_run():
    run = make_run()
    report = lint_runs([run])
    assert not report.has_issues
    assert "No lint issues" in str(report)


def test_failed_no_error():
    run = make_run(status=PipelineStatus.FAILED, error=None)
    report = lint_runs([run])
    codes = {i.code for i in report.issues}
    assert "FAILED_NO_ERROR" in codes


def test_negative_duration():
    run = make_run()
    run.duration = -5.0
    report = lint_runs([run])
    codes = {i.code for i in report.issues}
    assert "NEG_DURATION" in codes


def test_time_inversion():
    run = make_run(
        started_at=datetime(2024, 1, 1, 10, 10, tzinfo=timezone.utc),
        finished_at=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
    )
    report = lint_runs([run])
    codes = {i.code for i in report.issues}
    assert "TIME_INVERSION" in codes


def test_success_with_error():
    run = make_run(status=PipelineStatus.SUCCESS, error="something")
    report = lint_runs([run])
    codes = {i.code for i in report.issues}
    assert "SUCCESS_WITH_ERROR" in codes


def test_empty_pipeline_name():
    run = make_run(pipeline_name="")
    report = lint_runs([run])
    codes = {i.code for i in report.issues}
    assert "EMPTY_NAME" in codes


def test_multiple_issues_same_run():
    run = make_run(status=PipelineStatus.FAILED, error=None)
    run.duration = -1.0
    report = lint_runs([run])
    assert len(report.issues) >= 2


def test_str_report_lists_issues():
    run = make_run(status=PipelineStatus.FAILED, error=None)
    report = lint_runs([run])
    text = str(report)
    assert "FAILED_NO_ERROR" in text
