"""Simple schedule-based check runner for pipewatch."""

import time
import logging
from dataclasses import dataclass, field
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ScheduledJob:
    name: str
    interval_seconds: int
    fn: Callable[[], None]
    last_run: Optional[float] = field(default=None)

    def is_due(self, now: float) -> bool:
        if self.last_run is None:
            return True
        return (now - self.last_run) >= self.interval_seconds

    def run(self, now: float) -> None:
        logger.info("Running scheduled job: %s", self.name)
        try:
            self.fn()
        except Exception as exc:  # noqa: BLE001
            logger.error("Job %s failed: %s", self.name, exc)
        self.last_run = now


class Scheduler:
    def __init__(self, tick: float = 1.0) -> None:
        self.tick = tick
        self._jobs: List[ScheduledJob] = []
        self._running = False

    def register(self, name: str, interval_seconds: int, fn: Callable[[], None]) -> None:
        job = ScheduledJob(name=name, interval_seconds=interval_seconds, fn=fn)
        self._jobs.append(job)
        logger.debug("Registered job '%s' every %ds", name, interval_seconds)

    def stop(self) -> None:
        self._running = False

    def run_once(self) -> None:
        now = time.monotonic()
        for job in self._jobs:
            if job.is_due(now):
                job.run(now)

    def start(self, max_iterations: Optional[int] = None) -> None:
        self._running = True
        iterations = 0
        while self._running:
            self.run_once()
            iterations += 1
            if max_iterations is not None and iterations >= max_iterations:
                break
            time.sleep(self.tick)
