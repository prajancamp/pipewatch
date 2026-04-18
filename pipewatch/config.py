"""Load pipewatch configuration from a TOML or JSON file."""
import json
import os
from dataclasses import dataclass, field
from typing import Optional
from pipewatch.alert import AlertRule

DEFAULT_CONFIG_PATH = os.path.expanduser("~/.pipewatch/config.json")


@dataclass
class PipewatchConfig:
    store_path: str = os.path.expanduser("~/.pipewatch/runs.jsonl")
    alert_rule: AlertRule = field(default_factory=AlertRule)
    tail_log: Optional[str] = None


def load_config(path: str = DEFAULT_CONFIG_PATH) -> PipewatchConfig:
    if not os.path.exists(path):
        return PipewatchConfig()
    with open(path) as f:
        data = json.load(f)
    alert_data = data.pop("alert_rule", {})
    rule = AlertRule(**{k: v for k, v in alert_data.items() if k in AlertRule.__dataclass_fields__})
    cfg_fields = {k: v for k, v in data.items() if k in PipewatchConfig.__dataclass_fields__ and k != "alert_rule"}
    return PipewatchConfig(alert_rule=rule, **cfg_fields)


def save_config(cfg: PipewatchConfig, path: str = DEFAULT_CONFIG_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = {
        "store_path": cfg.store_path,
        "tail_log": cfg.tail_log,
        "alert_rule": {
            "consecutive_failures": cfg.alert_rule.consecutive_failures,
            "min_success_rate": cfg.alert_rule.min_success_rate,
            "max_avg_duration": cfg.alert_rule.max_avg_duration,
        },
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
