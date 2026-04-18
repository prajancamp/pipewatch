"""Tests for pipewatch.config module."""
import json
import os
import pytest
from pipewatch.config import PipewatchConfig, load_config, save_config
from pipewatch.alert import AlertRule


@pytest.fixture
def cfg_path(tmp_path):
    return str(tmp_path / "config.json")


def test_load_missing_returns_defaults(tmp_path):
    cfg = load_config(str(tmp_path / "nonexistent.json"))
    assert isinstance(cfg, PipewatchConfig)
    assert isinstance(cfg.alert_rule, AlertRule)


def test_save_and_load_roundtrip(cfg_path):
    rule = AlertRule(consecutive_failures=5, min_success_rate=0.9, max_avg_duration=60.0)
    cfg = PipewatchConfig(store_path="/tmp/runs.jsonl", alert_rule=rule, tail_log="/var/log/etl.log")
    save_config(cfg, cfg_path)
    loaded = load_config(cfg_path)
    assert loaded.store_path == "/tmp/runs.jsonl"
    assert loaded.tail_log == "/var/log/etl.log"
    assert loaded.alert_rule.consecutive_failures == 5
    assert loaded.alert_rule.min_success_rate == 0.9
    assert loaded.alert_rule.max_avg_duration == 60.0


def test_save_creates_directory(tmp_path):
    path = str(tmp_path / "subdir" / "config.json")
    save_config(PipewatchConfig(), path)
    assert os.path.exists(path)


def test_load_partial_config(cfg_path):
    with open(cfg_path, "w") as f:
        json.dump({"store_path": "/custom/runs.jsonl"}, f)
    cfg = load_config(cfg_path)
    assert cfg.store_path == "/custom/runs.jsonl"
    assert cfg.alert_rule.consecutive_failures == 3
