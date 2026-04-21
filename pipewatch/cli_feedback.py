"""CLI subcommands for managing alert feedback (ack / resolve / suppress)."""

from __future__ import annotations

import argparse

from pipewatch.feedback import (
    add_feedback,
    load_feedback,
    suppressed_keys,
)

VALID_ACTIONS = ("acknowledged", "resolved", "suppressed")


def cmd_feedback_add(args: argparse.Namespace) -> None:
    action = args.action
    if action not in VALID_ACTIONS:
        print(f"[error] action must be one of: {', '.join(VALID_ACTIONS)}")
        raise SystemExit(1)
    entry = add_feedback(
        store_path=args.store,
        alert_key=args.alert_key,
        action=action,
        note=args.note,
    )
    print(f"Recorded: {entry}")


def cmd_feedback_list(args: argparse.Namespace) -> None:
    entries = load_feedback(args.store)
    if not entries:
        print("No feedback entries recorded.")
        return
    for entry in entries:
        print(entry)


def cmd_feedback_suppressed(args: argparse.Namespace) -> None:
    keys = suppressed_keys(args.store)
    if not keys:
        print("No suppressed alerts.")
        return
    print(f"{'Alert Key':<40} {'Since':<30} {'Note'}")
    print("-" * 80)
    for key, entry in sorted(keys.items()):
        note = entry.note or ""
        print(f"{key:<40} {entry.timestamp:<30} {note}")


def register_feedback_subcommands(sub: argparse._SubParsersAction) -> None:
    p_fb = sub.add_parser("feedback", help="Manage alert feedback")
    fb_sub = p_fb.add_subparsers(dest="feedback_cmd")

    p_add = fb_sub.add_parser("add", help="Record feedback for an alert")
    p_add.add_argument("alert_key", help="Alert key, e.g. my_pipeline:consecutive_failures")
    p_add.add_argument("action", help=f"One of: {', '.join(VALID_ACTIONS)}")
    p_add.add_argument("--note", default=None, help="Optional note")
    p_add.set_defaults(func=cmd_feedback_add)

    p_list = fb_sub.add_parser("list", help="List all feedback entries")
    p_list.set_defaults(func=cmd_feedback_list)

    p_sup = fb_sub.add_parser("suppressed", help="Show currently suppressed alert keys")
    p_sup.set_defaults(func=cmd_feedback_suppressed)
