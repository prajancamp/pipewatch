"""Annotation support: attach notes to pipeline runs."""
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, List, Optional


def _annotations_path(store_path: str) -> Path:
    return Path(store_path).parent / "annotations.json"


def load_annotations(store_path: str) -> Dict[str, List[str]]:
    """Return mapping of run_id -> list of note strings."""
    path = _annotations_path(store_path)
    if not path.exists():
        return {}
    with path.open() as f:
        return json.load(f)


def save_annotations(store_path: str, annotations: Dict[str, List[str]]) -> None:
    path = _annotations_path(store_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(annotations, f, indent=2)


def add_annotation(store_path: str, run_id: str, note: str) -> None:
    """Append a note to a run."""
    annotations = load_annotations(store_path)
    annotations.setdefault(run_id, []).append(note)
    save_annotations(store_path, annotations)


def get_annotations(store_path: str, run_id: str) -> List[str]:
    """Return all notes for a given run_id."""
    return load_annotations(store_path).get(run_id, [])


def remove_annotations(store_path: str, run_id: str) -> bool:
    """Delete all annotations for a run. Returns True if any existed."""
    annotations = load_annotations(store_path)
    if run_id not in annotations:
        return False
    del annotations[run_id]
    save_annotations(store_path, annotations)
    return True


def annotated_run_ids(store_path: str) -> List[str]:
    """Return all run IDs that have at least one annotation."""
    return list(load_annotations(store_path).keys())
