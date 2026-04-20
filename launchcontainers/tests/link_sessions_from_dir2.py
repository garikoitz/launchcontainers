#!/usr/bin/env python3
# -----------------------------------------------------------------------------
# Copyright (c) Yongning Lei 2024-2025
# MIT License
# -----------------------------------------------------------------------------
"""
Create symlinks in dir1 for sessions that exist in dir2 but not in dir1.

Structure assumed (same in both dirs)::

    <dir>/sub-XX/ses-YY/

For every sub-XX/ses-YY found in dir2, if the corresponding path does not
already exist in dir1, a symlink is created::

    dir1/sub-XX/ses-YY  →  dir2/sub-XX/ses-YY

The sub-XX parent directory in dir1 is created if needed.

Usage (dry-run preview)::

    python link_sessions_from_dir2.py --dir1 /path/to/dir1 --dir2 /path/to/dir2

Execute (write symlinks)::

    python link_sessions_from_dir2.py --dir1 /path/to/dir1 --dir2 /path/to/dir2 --execute
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich import box
from rich.table import Table

console = Console()
app = typer.Typer(add_completion=False, pretty_exceptions_show_locals=False)


@app.command()
def main(
    dir1: Path = typer.Option(..., "--dir1", help="Target directory (will receive symlinks)"),
    dir2: Path = typer.Option(..., "--dir2", help="Source directory (sessions to link from)"),
    execute: bool = typer.Option(False, "--execute", help="Write symlinks (default: dry-run preview)"),
    absolute: bool = typer.Option(
        False, "--absolute",
        help="Use absolute symlink targets (default: relative paths)",
    ),
) -> None:
    """
    Symlink sessions from dir2 into dir1, preserving sub-XX/ses-YY structure.
    """
    if not dir1.is_dir():
        console.print(f"[red]ERROR[/red]: dir1 does not exist: {dir1}")
        raise typer.Exit(1)
    if not dir2.is_dir():
        console.print(f"[red]ERROR[/red]: dir2 does not exist: {dir2}")
        raise typer.Exit(1)

    mode_str = "[green]EXECUTE[/green]" if execute else "[yellow]DRY-RUN[/yellow]"
    console.rule("[bold cyan]link_sessions_from_dir2[/bold cyan]")
    console.print(
        f"  dir1 (target)  : {dir1}\n"
        f"  dir2 (source)  : {dir2}\n"
        f"  Mode           : {mode_str}\n"
    )

    n_linked  = 0
    n_exists  = 0
    n_conflict = 0

    rows: list[tuple[str, str, str, str]] = []  # sub, ses, action, note

    # Scan dir2 for sub-*/ses-* pairs
    for sub_dir in sorted(dir2.glob("sub-*")):
        if not sub_dir.is_dir():
            continue
        sub = sub_dir.name  # e.g. "sub-04"

        for ses_dir in sorted(sub_dir.glob("ses-*")):
            if not ses_dir.is_dir():
                continue
            ses = ses_dir.name  # e.g. "ses-09"

            tgt = dir1 / sub / ses          # where symlink will live
            src = dir2 / sub / ses          # what it points to

            if tgt.exists() or tgt.is_symlink():
                if tgt.is_symlink():
                    existing = Path(tgt.readlink() if hasattr(tgt, "readlink") else tgt.resolve())
                    note = f"symlink → {tgt.readlink() if hasattr(tgt, 'readlink') else existing}"
                    rows.append((sub, ses, "[dim]SKIP (symlink exists)[/dim]", note))
                else:
                    rows.append((sub, ses, "[dim]SKIP (real dir)[/dim]", str(tgt)))
                n_exists += 1
                continue

            # Determine symlink target path
            if absolute:
                link_target = src.resolve()
            else:
                # Relative: from tgt.parent (dir1/sub-XX/) to src
                link_target = Path("..") / ".." / dir2.name / sub / ses
                # More robust: compute proper relative path
                link_target = Path(
                    *[".."] * len(tgt.parent.relative_to(dir1).parts),
                    *src.parts[len(dir1.parts):]  # works only if dir1/dir2 share parent
                )
                # Safest: always compute correctly regardless of depth
                try:
                    link_target = src.resolve().relative_to(tgt.parent.resolve())
                except ValueError:
                    # Not a subdirectory relationship; fall back to absolute
                    link_target = src.resolve()

            rows.append((sub, ses, "[green]LINK[/green]", f"→ {link_target}"))
            n_linked += 1

            if execute:
                tgt.parent.mkdir(parents=True, exist_ok=True)
                tgt.symlink_to(link_target)

    # Print table
    tbl = Table(title="Session link summary", box=box.SIMPLE_HEAD)
    tbl.add_column("sub")
    tbl.add_column("ses")
    tbl.add_column("action")
    tbl.add_column("note / target")
    for sub, ses, action, note in rows:
        tbl.add_row(sub, ses, action, note)
    console.print(tbl)

    console.print(
        f"\n  To link  : [bold]{n_linked}[/bold]\n"
        f"  Skipped  : [bold]{n_exists}[/bold]\n"
    )

    if not execute:
        console.print("[dim]  Dry-run — nothing written. Pass --execute to create symlinks.[/dim]")
    else:
        console.print(f"[green]  Done.[/green] {n_linked} symlink(s) created.")


if __name__ == "__main__":
    app()
