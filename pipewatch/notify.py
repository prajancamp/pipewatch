"""Notification sinks for pipewatch alerts."""
import json
import urllib.request
from typing import List, Optional
from pipewatch.alert import Alert


def notify_stdout(alerts: List[Alert]) -> None:
    """Print alerts to stdout."""
    for alert in alerts:
        print(str(alert))


def notify_webhook(alerts: List[Alert], url: str, timeout: int = 5) -> bool:
    """POST alerts as JSON to a webhook URL. Returns True on success."""
    if not alerts:
        return True
    payload = json.dumps({
        "alerts": [
            {"pipeline": a.pipeline, "level": a.level, "message": a.message}
            for a in alerts
        ]
    }).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status < 400
    except Exception:
        return False


def notify_file(alerts: List[Alert], path: str) -> None:
    """Append alerts as JSONL to a file."""
    with open(path, "a") as f:
        for a in alerts:
            f.write(json.dumps({"pipeline": a.pipeline, "level": a.level, "message": a.message}) + "\n")
