"""
MIT License
Copyright (c) 2024-2025 Yongning Lei

Remove *_events.tsv files from BIDS raw func directories that have no matching
bold.nii.gz, preventing downstream containers from trying to process tasks
that were never actually acquired.

Operates only on raw BIDS (sub-*/ses-*/func/) — never touches derivatives.

Usage examples
--------------
  # dry-run for the whole dataset
  python 04_clean_orphan_events_tsv.py -b /path/BIDS

  # dry-run for one subject / session
  python 04_clean_orphan_events_tsv.py -b /path/BIDS -s 11 --ses 10

  # dry-run from subseslist
  python 04_clean_orphan_events_tsv.py -b /path/BIDS -f subseslist.txt

  # actually delete (move to backup dir)
  python 04_clean_orphan_events_tsv.py -b /path/BIDS --no-dry-run
"""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from launchcontainers.utils import parse_subses_list

console = Console()
app = typer.Typer(pretty_exceptions_show_locals=False)


def _find_orphan_events(func_dir: Path) -> list[Path]:
    """Return events.tsv files whose matching bold.nii.gz does not exist."""
    orphans = []
    for ev in sorted(func_dir.glob("*_events.tsv")):
        bold = Path(str(ev).replace("_events.tsv", "_bold.nii.gz"))
        if not bold.exists():
            orphans.append(ev)
    return orphans


@app.command()
def main(
    bids_dir: Path = typer.Option(..., "--bids", "-b", help="BIDS root directory."),
    sub: Optional[str] = typer.Option(None, "--sub", "-s", help="Subject ID (e.g. 11)."),
    ses: Optional[str] = typer.Option(None, "--ses", help="Session ID (e.g. 10)."),
    file: Optional[Path] = typer.Option(
        None, "--file", "-f", help="Subseslist CSV/TSV with sub,ses columns."
    ),
    dry_run: bool = typer.Option(
        True,
        "--dry-run/--no-dry-run",
        help="Preview only (default). Use --no-dry-run to move files to backup dirs.",
    ),
) -> None:
    """Remove orphaned *_events.tsv files (no matching bold.nii.gz) from BIDS func dirs."""

    # ------------------------------------------------------------------
    # Resolve sub/ses pairs
    # ------------------------------------------------------------------
    if file is None and sub is None:
        # No filter — scan the entire dataset
        pairs = [
            (p.parent.parent.name.removeprefix("sub-"), p.parent.name.removeprefix("ses-"))
            for p in sorted(bids_dir.glob("sub-*/ses-*/func"))
            if p.is_dir() and "derivatives" not in str(p)
        ]
    elif file is not None:
        pairs = parse_subses_list(file)
    else:
        if ses is None:
            raise typer.BadParameter("Provide --ses when using --sub")
        pairs = [(sub.zfill(2), ses.zfill(2))]

    mode = "[yellow]DRY-RUN[/yellow]" if dry_run else "[red]DELETE (backup)[/red]"
    console.print(f"\n[bold]Orphan events.tsv cleanup[/bold]  |  mode: {mode}")
    console.print(f"BIDS: {bids_dir}\n")

    total_found = 0
    total_acted = 0

    for sub_id, ses_id in pairs:
        func_dir = bids_dir / f"sub-{sub_id}" / f"ses-{ses_id}" / "func"
        if not func_dir.exists():
            continue

        orphans = _find_orphan_events(func_dir)
        if not orphans:
            continue

        console.print(f"[cyan]sub-{sub_id} ses-{ses_id}[/cyan] — {len(orphans)} orphan(s):")
        total_found += len(orphans)

        for ev in orphans:
            console.print(f"  {'[dim]would remove[/dim]' if dry_run else '[red]removing[/red]'} {ev.name}")
            if not dry_run:
                backup_dir = func_dir.parent / "func_backup_orphan_events"
                backup_dir.mkdir(exist_ok=True)
                shutil.move(str(ev), str(backup_dir / ev.name))
                total_acted += 1

    console.print(f"\n[bold]Done.[/bold] Found {total_found} orphan file(s).", end="")
    if dry_run:
        console.print(" Re-run with [bold]--no-dry-run[/bold] to move them to backup dirs.")
    else:
        console.print(f" Moved {total_acted} file(s) to [dim]func_backup_orphan_events/[/dim] dirs.")


if __name__ == "__main__":
    app()
