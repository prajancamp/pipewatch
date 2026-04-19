import pytest
from pipewatch.analyzer import PipelineStats
from pipewatch.scorecard import score_pipeline, build_scorecard, _grade, ScorecardEntry


def make_stats(
    pipeline="pipe",
    total=10,
    failures=0,
    consecutive=0,
    avg_duration=None,
):
    success = total - failures
    return PipelineStats(
        pipeline=pipeline,
        total_runs=total,
        failed_runs=failures,
        success_rate=success / total if total else 0.0,
        consecutive_failures=consecutive,
        avg_duration_seconds=avg_duration,
    )


def test_grade_boundaries():
    assert _grade(95) == "A"
    assert _grade(80) == "B"
    assert _grade(65) == "C"
    assert _grade(50) == "D"
    assert _grade(30) == "F"


def test_score_perfect_pipeline():
    stats = make_stats(total=20, failures=0)
    entry = score_pipeline(stats)
    assert entry.score == 100.0
    assert entry.grade == "A"
    assert entry.reasons == []


def test_score_low_success_rate():
    stats = make_stats(total=10, failures=8)
    entry = score_pipeline(stats)
    assert entry.score <= 60
    assert any("success rate" in r.lower() for r in entry.reasons)


def test_score_consecutive_failures():
    stats = make_stats(total=10, failures=2, consecutive=5)
    entry = score_pipeline(stats)
    assert entry.score < 100
    assert any("consecutive" in r for r in entry.reasons)


def test_score_high_duration():
    stats = make_stats(total=10, failures=0, avg_duration=7200)
    entry = score_pipeline(stats)
    assert entry.score < 100
    assert any("duration" in r for r in entry.reasons)


def test_score_no_runs():
    stats = make_stats(total=0, failures=0)
    entry = score_pipeline(stats)
    assert entry.score == 0.0
    assert entry.grade == "F"


def test_build_scorecard_sorted_ascending():
    s1 = make_stats(pipeline="good", total=10, failures=0)
    s2 = make_stats(pipeline="bad", total=10, failures=9)
    entries = build_scorecard([s1, s2])
    assert entries[0].pipeline == "bad"
    assert entries[-1].pipeline == "good"


def test_score_clamped_to_zero():
    stats = make_stats(total=10, failures=10, consecutive=5, avg_duration=9999)
    entry = score_pipeline(stats)
    assert entry.score >= 0.0
