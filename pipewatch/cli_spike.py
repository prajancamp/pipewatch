"""CLI subcommand: pipewatch spike — detect sudden failure bursts."""
from __future__ import annotations

import argparse

from pipewatch.spike import detect_spikes
from pipewatch.store import RunStore


def cmd_spike(args: argparse.Namespace) -> None:
    store = RunStore(args.store)
    runs = store.load_all()

    results = detect_spikes(
        runs,
        window_minutes=args.window,
        lookback_minutes=args.lookback,
        threshold_multiplier=args.multiplier,
        min_spike_count=args.min_count,
        pipeline=getattr(args, "pipeline", None),
    )

    if not results:
        print("No pipeline activity found in the lookback window.")
        return

    flagged = [r for r in results if r.flagged]
    ok = [r for r in results if not r.flagged]

    if flagged:
        print(f"{'='*50}")
        print(f"  FAILURE SPIKES DETECTED ({len(flagged)})")  
        print(f"{'='*50}")
        for r in flagged:
            print(f"  {r}")
        print()

    if ok and args.verbose:
        print("Healthy pipelines:")
        for r in ok:
            print(f"  {r}")

    if not flagged:
        print("No spikes detected — all pipelines within normal failure range.")


def register_spike_subcommands(sub: argparse.Action) -> None:  # type: ignore[type-arg]
    p = sub.add_parser("spike", help="Detect sudden failure spikes")
    p.add_argument("--window", type=int, default=30, help="Recent window in minutes (default: 30)")
    p.add_argument("--lookback", type=int, default=360, help="Baseline lookback in minutes (default: 360)")
    p.add_argument("--multiplier", type=float, default=2.0, help="Spike threshold multiplier (default: 2.0)")
    p.add_argument("--min-count", type=int, default=2, dest="min_count", help="Min failures to flag (default: 2)")
    p.add_argument("--pipeline", default=None, help="Filter to a single pipeline")
    p.add_argument("--verbose", action="store_true", help="Also show healthy pipelines")
    p.set_defaults(func=cmd_spike)
