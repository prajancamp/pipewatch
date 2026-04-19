"""Tests for pipewatch.annotate."""
import pytest
from pipewatch.annotate import (
    add_annotation,
    get_annotations,
    remove_annotations,
    annotated_run_ids,
    load_annotations,
)


@pytest.fixture
def store_path(tmp_path):
    return str(tmp_path / "runs" / "runs.jsonl")


def test_get_annotations_missing(store_path):
    assert get_annotations(store_path, "run-1") == []


def test_add_and_get_annotation(store_path):
    add_annotation(store_path, "run-1", "looks suspicious")
    notes = get_annotations(store_path, "run-1")
    assert notes == ["looks suspicious"]


def test_add_multiple_annotations(store_path):
    add_annotation(store_path, "run-1", "first note")
    add_annotation(store_path, "run-1", "second note")
    notes = get_annotations(store_path, "run-1")
    assert len(notes) == 2
    assert "first note" in notes
    assert "second note" in notes


def test_annotations_isolated_by_run(store_path):
    add_annotation(store_path, "run-1", "note for 1")
    add_annotation(store_path, "run-2", "note for 2")
    assert get_annotations(store_path, "run-1") == ["note for 1"]
    assert get_annotations(store_path, "run-2") == ["note for 2"]


def test_remove_annotations(store_path):
    add_annotation(store_path, "run-1", "temp note")
    removed = remove_annotations(store_path, "run-1")
    assert removed is True
    assert get_annotations(store_path, "run-1") == []


def test_remove_missing_returns_false(store_path):
    assert remove_annotations(store_path, "nonexistent") is False


def test_annotated_run_ids(store_path):
    add_annotation(store_path, "run-a", "x")
    add_annotation(store_path, "run-b", "y")
    ids = annotated_run_ids(store_path)
    assert set(ids) == {"run-a", "run-b"}


def test_annotated_run_ids_empty(store_path):
    assert annotated_run_ids(store_path) == []
