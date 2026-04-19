"""Tests for pipewatch.scheduler."""

import time
import pytest
from unittest.mock import MagicMock, patch
from pipewatch.scheduler import Scheduler, ScheduledJob


def test_scheduled_job_due_on_first_run():
    job = ScheduledJob(name="j", interval_seconds=60, fn=lambda: None)
    assert job.is_due(time.monotonic())


def test_scheduled_job_not_due_before_interval():
    now = time.monotonic()
    job = ScheduledJob(name="j", interval_seconds=60, fn=lambda: None, last_run=now)
    assert not job.is_due(now + 10)


def test_scheduled_job_due_after_interval():
    now = time.monotonic()
    job = ScheduledJob(name="j", interval_seconds=30, fn=lambda: None, last_run=now)
    assert job.is_due(now + 31)


def test_job_run_updates_last_run():
    mock_fn = MagicMock()
    now = time.monotonic()
    job = ScheduledJob(name="j", interval_seconds=10, fn=mock_fn)
    job.run(now)
    mock_fn.assert_called_once()
    assert job.last_run == now


def test_job_run_swallows_exceptions():
    def boom():
        raise RuntimeError("oops")

    job = ScheduledJob(name="j", interval_seconds=10, fn=boom)
    # should not raise
    job.run(time.monotonic())


def test_scheduler_register_and_run_once():
    mock_fn = MagicMock()
    s = Scheduler(tick=0.01)
    s.register("check", 0, mock_fn)
    s.run_once()
    mock_fn.assert_called_once()


def test_scheduler_respects_interval():
    mock_fn = MagicMock()
    s = Scheduler(tick=0.01)
    s.register("check", 9999, mock_fn)
    # Force last_run to now so it won't be due
    s._jobs[0].last_run = time.monotonic()
    s.run_once()
    mock_fn.assert_not_called()


def test_scheduler_start_max_iterations():
    mock_fn = MagicMock()
    s = Scheduler(tick=0.0)
    s.register("check", 0, mock_fn)
    s.start(max_iterations=3)
    assert mock_fn.call_count == 3


def test_scheduler_stop():
    s = Scheduler(tick=0.0)
    call_count = {"n": 0}

    def stopper():
        call_count["n"] += 1
        if call_count["n"] >= 2:
            s.stop()

    s.register("stopper", 0, stopper)
    s.start()
    assert call_count["n"] == 2
