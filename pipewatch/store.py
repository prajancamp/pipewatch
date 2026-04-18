import json
import os
from pathlib import Path
from typing import List, Optional

from pipewatch.models import PipelineRun, PipelineStatus


DEFAULT_STORE_PATH = Path.home() / ".pipewatch" / "runs.jsonl"


class RunStore:
    def __init__(self, store_path: Optional[Path] = None):
        self.store_path = Path(store_path or DEFAULT_STORE_PATH)
        self.store_path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, run: PipelineRun) -> None:
        with open(self.store_path, "a") as f:
            f.write(json.dumps(run.to_dict()) + "\n")

    def load_all(self) -> List[PipelineRun]:
        if not self.store_path.exists():
            return []
        runs = []
        with open(self.store_path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        runs.append(PipelineRun.from_dict(json.loads(line)))
                    except (KeyError, ValueError):
                        continue
        return runs

    def load_by_pipeline(self, pipeline_id: str) -> List[PipelineRun]:
        return [r for r in self.load_all() if r.pipeline_id == pipeline_id]

    def load_failures(self, pipeline_id: Optional[str] = None) -> List[PipelineRun]:
        runs = self.load_all()
        failures = [r for r in runs if r.is_failed]
        if pipeline_id:
            failures = [r for r in failures if r.pipeline_id == pipeline_id]
        return failures

    def clear(self) -> None:
        if self.store_path.exists():
            os.remove(self.store_path)
