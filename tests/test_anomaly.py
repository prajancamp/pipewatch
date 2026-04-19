import pytest
from datetime import datetime, timezone
from pipewatch.models import PipelineRun, PipelineStatus
from pipewatch.anomaly import detect_anomalies, detect_duration_anomalies, AnomalyResult


def make_run(pipeline: str, duration: float, status: PipelineStatus = PipelineStatus.SUCCESS, run_id: str = None) -> PipelineRun:
    import uuid
    now = datetime.now(timezone.utc).isoformat()
    return PipelineRun(
        run_id=run_id or str(uuid.uuid4()),
        pipeline=pipeline,
        status=status,
        started_at=now,
        ended_at=now,
        duration=duration,
    )


def test_no_anomalies_uniform_durations():
    runs = [make_run("etl", 10.0) for _ in range(5)]
    results = detect_anomalies(runs)
    assert results == []


def test_detects_outlier_duration():
    runs = [make_run("etl", 10.0) for _ in range(8)]
    outlier = make_run("etl", 500.0, run_id="outlier-run")
    runs.append(outlier)
    results = detect_anomalies(runs)
    assert any(r.run_id == "outlier-run" for r in results)


def test_insufficient_data_skipped():
    runs = [make_run("etl", 10.0), make_run("etl", 20.0)]
    results = detect_anomalies(runs)
    assert results == []


def test_zero_stddev_skipped():
    runs = [make_run("etl", 10.0) for _ in range(5)]
    results = detect_anomalies(runs)
    assert results == []


def test_anomaly_result_str():
    a = AnomalyResult(pipeline="etl", run_id="abc", reason="too slow", duration=500.0, avg_duration=10.0)
    assert "ANOMALY" in str(a)
    assert "etl" in str(a)
    assert "abc" in str(a)


def test_multiple_pipelines_isolated():
    runs_a = [make_run("pipe-a", 10.0) for _ in range(8)]
    runs_a.append(make_run("pipe-a", 999.0, run_id="bad-a"))
    runs_b = [make_run("pipe-b", 100.0) for _ in range(8)]
    results = detect_anomalies(runs_a + runs_b)
    pipelines = {r.pipeline for r in results}
    assert "pipe-a" in pipelines
    assert "pipe-b" not in pipelines


def test_custom_z_threshold():
    runs = [make_run("etl", float(i)) for i in range(1, 9)]
    runs.append(make_run("etl", 50.0, run_id="mild-outlier"))
    strict = detect_anomalies(runs, z_threshold=1.0)
    lenient = detect_anomalies(runs, z_threshold=5.0)
    assert len(strict) >= len(lenient)
