# pipewatch

Lightweight CLI monitor for tracking ETL pipeline health and failure patterns.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install -e .
```

---

## Usage

```bash
# Watch a pipeline log file for failures
pipewatch watch --log pipeline.log

# Show a summary of failure patterns over the last 24 hours
pipewatch report --since 24h

# Monitor a live pipeline with alerts on repeated failures
pipewatch watch --log pipeline.log --alert-threshold 3
```

Example output:

```
[pipewatch] 12:45:01  ✓ Stage: extract        (2.3s)
[pipewatch] 12:45:04  ✓ Stage: transform      (1.1s)
[pipewatch] 12:45:06  ✗ Stage: load           FAILED — connection timeout (x3)
[pipewatch] ⚠ Failure pattern detected: "connection timeout" — 3 occurrences in 10m
```

---

## Configuration

Optionally create a `pipewatch.toml` in your project root:

```toml
[monitor]
alert_threshold = 3
window_minutes = 10
log_format = "auto"
```

---

## Requirements

- Python 3.8+
- No external dependencies required for core functionality

---

## License

MIT © 2024 [yourname](https://github.com/yourname)