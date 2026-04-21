"""CLI subcommands for annotating pipeline runs."""
from __future__ import annotations
import argparse
from pipewatch.annotate import add_annotation, get_annotations, remove_annotations, annotated_run_ids


def cmd_annotate_add(args: argparse.Namespace) -> None:
    """Add a note annotation to a specific run."""
    add_annotation(args.store, args.run_id, args.note)
    print(f"Annotation added to run '{args.run_id}'.")


def cmd_annotate_list(args: argparse.Namespace) -> None:
    """List all annotations for a specific run."""
    notes = get_annotations(args.store, args.run_id)
    if not notes:
        print(f"No annotations for run '{args.run_id}'.")
        return
    for i, note in enumerate(notes, 1):
        print(f"  [{i}] {note}")


def cmd_annotate_remove(args: argparse.Namespace) -> None:
    """Remove all annotations for a specific run."""
    removed = remove_annotations(args.store, args.run_id)
    if removed:
        print(f"Annotations removed for run '{args.run_id}'.")
    else:
        print(f"No annotations found for run '{args.run_id}'.")


def cmd_annotate_all(args: argparse.Namespace) -> None:
    """List all runs that have at least one annotation."""
    ids = annotated_run_ids(args.store)
    if not ids:
        print("No annotated runs.")
        return
    for run_id in ids:
        notes = get_annotations(args.store, run_id)
        print(f"{run_id}: {len(notes)} note(s)")


def cmd_annotate_show(args: argparse.Namespace) -> None:
    """Show all annotations for all annotated runs, with full note text."""
    ids = annotated_run_ids(args.store)
    if not ids:
        print("No annotated runs.")
        return
    for run_id in ids:
        notes = get_annotations(args.store, run_id)
        print(f"{run_id}:")
        for i, note in enumerate(notes, 1):
            print(f"  [{i}] {note}")


def register_annotate_subcommands(subparsers, store_default: str) -> None:
    """Register the 'annotate' subcommand and its sub-subcommands."""
    ann = subparsers.add_parser("annotate", help="Manage run annotations")
    ann.add_argument("--store", default=store_default)
    ann_sub = ann.add_subparsers(dest="annotate_cmd")

    p_add = ann_sub.add_parser("add", help="Add a note to a run")
    p_add.add_argument("run_id")
    p_add.add_argument("note")
    p_add.set_defaults(func=cmd_annotate_add)

    p_list = ann_sub.add_parser("list", help="List notes for a run")
    p_list.add_argument("run_id")
    p_list.set_defaults(func=cmd_annotate_list)

    p_rm = ann_sub.add_parser("remove", help="Remove all notes for a run")
    p_rm.add_argument("run_id")
    p_rm.set_defaults(func=cmd_annotate_remove)

    p_all = ann_sub.add_parser("all", help="List all annotated runs")
    p_all.set_defaults(func=cmd_annotate_all)

    p_show = ann_sub.add_parser("show", help="Show full annotations for all annotated runs")
    p_show.set_defaults(func=cmd_annotate_show)
