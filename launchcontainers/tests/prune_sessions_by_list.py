#!/usr/bin/env python3
# -----------------------------------------------------------------------------
# Copyright (c) Yongning Lei 2024-2025
# MIT License
# -----------------------------------------------------------------------------
"""
Remove sessions from a directory that are NOT in a subseslist.

Reads a subseslist (CSV or TSV with sub/ses columns), then scans the target
directory for sub-XX/ses-YY directories. Any session not in the list is
deleted (or, if it is a symlink, unlinked).

Only rows where RUN == True are kept (same convention as other prepare scripts).

Usage (dry-run preview)::

    python prune_sessions_by_list.py -f subseslist.csv --dir /path/to/dir

Execute (delete sessions not in list)::

    python prune_sessions_by_list.py -f subseslist.csv --dir /path/to/dir --execute
"""
from __future__ import annotations

import csv
import shutil
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich import box
from rich.table import Table

console = Console()
app = typer.Typer(add_completion=False, pretty_exceptions_show_locals=False)


def _load_keep_set(list_path: Path) -> set[tuple[str, str]]:
    """Return set of (sub, ses) pairs to KEEP (zero-padded, RUN==True only)."""
    delimiter = "\t" if list_path.suffix.lower() == ".tsv" else ","
    keep: set[tuple[str, str]] = set()
    with open(list_path, newline="") as fh:
        for row in csv.DictReader(fh, delimiter=delimiter):
            if "RUN" in row and str(row["RUN"]).strip() != "True":
                continue
            sub = str(row["sub"]).strip().zfill(2)
            ses = str(row["ses"]).strip().zfill(2)
            keep.add((sub, ses))
    return keep


@app.command()
def main(
    list_file: Path = typer.Option(..., "-f", "--file", help="Subseslist CSV/TSV (sub, ses, RUN columns)"),
    target_dir: Path = typer.Option(..., "--dir", "-d", help="Directory with sub-XX/ses-YY structure to prune"),
    execute: bool = typer.Option(False, "--execute", help="Actually delete sessions (default: dry-run preview)"),
) -> None:
    """
    Delete sessions from --dir that are NOT present in the subseslist.
    Symlinks are unlinked; real directories are removed recursively.
    """
    if not list_file.exists():
        console.print(f"[red]ERROR[/red]: subseslist not found: {list_file}")
        raise typer.Exit(1)
    if not target_dir.is_dir():
        console.print(f"[red]ERROR[/red]: target directory not found: {target_dir}")
        raise typer.Exit(1)

    keep = _load_keep_set(list_file)

    mode_str = "[green]EXECUTE[/green]" if execute else "[yellow]DRY-RUN[/yellow]"
    console.rule("[bold cyan]prune_sessions_by_list[/bold cyan]")
    console.print(
        f"  Subseslist     : {list_file}  ({len(keep)} sessions to keep)\n"
        f"  Target dir     : {target_dir}\n"
        f"  Mode           : {mode_str}\n"
    )

    rows: list[tuple[str, str, str, str]] = []   # sub, ses, action, note
    n_keep  = 0
    n_drop  = 0

    for sub_dir in sorted(target_dir.glob("sub-*")):
        if not sub_dir.is_dir():
            continue
        sub_num = sub_dir.name.replace("sub-", "").zfill(2)

        for ses_path in sorted(sub_dir.glob("ses-*")):
            # ses-* can be a real dir or a symlink
            if not ses_path.exists() and not ses_path.is_symlink():
                continue
            ses_num = ses_path.name.replace("ses-", "").zfill(2)

            if (sub_num, ses_num) in keep:
                kind = "symlink" if ses_path.is_symlink() else "dir"
                rows.append((sub_dir.name, ses_path.name, "[dim]KEEP[/dim]", kind))
                n_keep += 1
            else:
                kind = "symlink" if ses_path.is_symlink() else "dir"
                rows.append((sub_dir.name, ses_path.name, "[bold red]DROP[/bold red]", kind))
                n_drop += 1
                if execute:
                    if ses_path.is_symlink():
                        ses_path.unlink()
                    else:
                        shutil.rmtree(ses_path)

    tbl = Table(title="Session prune plan", box=box.SIMPLE_HEAD)
    tbl.add_column("sub")
    tbl.add_column("ses")
    tbl.add_column("action")
    tbl.add_column("type")
    for sub, ses, action, note in rows:
        tbl.add_row(sub, ses, action, note)
    console.print(tbl)

    console.print(
        f"\n  Keep : [bold]{n_keep}[/bold]\n"
        f"  Drop : [bold]{n_drop}[/bold]\n"
    )

    if not execute:
        console.print("[dim]  Dry-run — nothing deleted. Pass --execute to remove sessions.[/dim]")
    else:
        console.print(f"[green]  Done.[/green] {n_drop} session(s) removed.")


if __name__ == "__main__":
    app()
