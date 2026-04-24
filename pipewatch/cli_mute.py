"""CLI subcommands for managing mute rules."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from pipewatch.mute import (
    MuteRule,
    add_mute_rule,
    is_muted,
    load_mute_rules,
    remove_expired_rules,
    save_mute_rules,
)


def cmd_mute_add(args: argparse.Namespace) -> None:
    expires_at = None
    if args.hours:
        expires_at = (datetime.now(timezone.utc) + timedelta(hours=args.hours)).isoformat()
    rule = MuteRule(pipeline=args.pipeline, reason=args.reason, expires_at=expires_at)
    add_mute_rule(args.store, rule)
    print(f"Muted: {rule}")


def cmd_mute_list(args: argparse.Namespace) -> None:
    rules = load_mute_rules(args.store)
    active = [r for r in rules if not r.is_expired()]
    if not active:
        print("No active mute rules.")
        return
    for r in active:
        print(f"  {r}")


def cmd_mute_remove(args: argparse.Namespace) -> None:
    rules = load_mute_rules(args.store)
    before = len(rules)
    rules = [r for r in rules if r.pipeline != args.pipeline]
    save_mute_rules(args.store, rules)
    removed = before - len(rules)
    print(f"Removed {removed} rule(s) for pipeline {args.pipeline!r}.")


def cmd_mute_prune(args: argparse.Namespace) -> None:
    n = remove_expired_rules(args.store)
    print(f"Pruned {n} expired mute rule(s).")


def cmd_mute_check(args: argparse.Namespace) -> None:
    rules = load_mute_rules(args.store)
    if is_muted(args.pipeline, rules):
        print(f"Pipeline {args.pipeline!r} is currently MUTED.")
    else:
        print(f"Pipeline {args.pipeline!r} is NOT muted.")


def register_mute_subcommands(sub: argparse._SubParsersAction, store_default: str) -> None:
    p_add = sub.add_parser("mute-add", help="Add a mute rule")
    p_add.add_argument("pipeline")
    p_add.add_argument("--reason", default="manually muted")
    p_add.add_argument("--hours", type=float, default=None, help="Expire after N hours")
    p_add.add_argument("--store", default=store_default)
    p_add.set_defaults(func=cmd_mute_add)

    p_list = sub.add_parser("mute-list", help="List active mute rules")
    p_list.add_argument("--store", default=store_default)
    p_list.set_defaults(func=cmd_mute_list)

    p_rm = sub.add_parser("mute-remove", help="Remove mute rules for a pipeline")
    p_rm.add_argument("pipeline")
    p_rm.add_argument("--store", default=store_default)
    p_rm.set_defaults(func=cmd_mute_remove)

    p_prune = sub.add_parser("mute-prune", help="Prune expired mute rules")
    p_prune.add_argument("--store", default=store_default)
    p_prune.set_defaults(func=cmd_mute_prune)

    p_check = sub.add_parser("mute-check", help="Check if a pipeline is muted")
    p_check.add_argument("pipeline")
    p_check.add_argument("--store", default=store_default)
    p_check.set_defaults(func=cmd_mute_check)
