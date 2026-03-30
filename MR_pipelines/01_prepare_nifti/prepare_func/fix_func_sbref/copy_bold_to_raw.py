"""
copy_bold_to_raw.py
-------------------
Copy ``*_bold*`` files from the BIDS func directory to the raw_nifti_sbref_fmap
func directory for one or more sub/ses pairs.

Source : ``<bidsdir>/sub-{sub}/ses-{ses}/func/*_bold*``
Dest   : ``<raw_dir>/sub-{sub}/ses-{ses}/func/``

CLI usage
---------
Single pair::

    python copy_bold_to_raw.py --bidsdir /path/to/BIDS \\
        --raw-dir /path/to/raw_nifti_sbref_fmap --sub 11 --ses 03

Batch::

    python copy_bold_to_raw.py --bidsdir /path/to/BIDS \\
        --raw-dir /path/to/raw_nifti_sbref_fmap --subses-list subses.txt
"""

from __future__ import annotations

import csv
import glob
import os
import os.path as op
import shutil
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

console = Console()
app = typer.Typer(pretty_exceptions_show_locals=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_subses_list(path: Path) -> list[tuple[str, str]]:
    """Read TSV / CSV / TXT (columns: sub, ses). Delimiter inferred from extension."""
    ext = path.suffix.lower()
    if ext == ".tsv":
        delimiter = "\t"
    elif ext in (".csv", ".txt"):
        delimiter = ","
    else:
        with open(path, newline="") as fh:
            sample = fh.read(1024)
        try:
            delimiter = csv.Sniffer().sniff(sample).delimiter
        except csv.Error:
            delimiter = ","

    pairs = []
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh, delimiter=delimiter)
        for row in reader:
            RUN = row.get("RUN", "").strip().replace("\r", "")
            if RUN and RUN != "False":
                continue
            pairs.append((str(row["sub"]).strip(), str(row["ses"]).strip()))
    return pairs


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------


def copy_bold_for_subses(
    bidsdir: str,
    raw_dir: str,
    sub: str,
    ses: str,
) -> list[str]:
    """
    Copy all ``*_bold*`` files for one sub/ses from BIDS to raw_nifti_sbref_fmap.

    Parameters
    ----------
    bidsdir : str
        BIDS root directory.
    raw_dir : str
        raw_nifti_sbref_fmap root directory.
    sub, ses : str
        Subject / session labels without prefix.

    Returns
    -------
    list[str]
        Destination paths of all files copied.
    """
    src_func = op.join(bidsdir, f"sub-{sub}", f"ses-{ses}", "func")
    dst_func = op.join(raw_dir, f"sub-{sub}", f"ses-{ses}", "func")
    os.makedirs(dst_func, exist_ok=True)

    bold_files = sorted(glob.glob(op.join(src_func, "*_bold*")))
    if not bold_files:
        console.print(f"  [yellow][WARN][/] No *_bold* files found in {src_func}")
        return []

    copied = []
    for src in bold_files:
        dst = op.join(dst_func, op.basename(src))
        shutil.copy2(src, dst)
        copied.append(dst)

    return copied


def batch_copy_bold(
    bidsdir: str,
    raw_dir: str,
    pairs: list[tuple[str, str]],
) -> dict[tuple[str, str], list[str]]:
    """
    Copy ``*_bold*`` files for a list of sub/ses pairs.

    Parameters
    ----------
    bidsdir, raw_dir : str
        BIDS root and raw_nifti_sbref_fmap root directories.
    pairs : list of (sub, ses)
        Subject/session pairs to process.

    Returns
    -------
    dict
        Maps ``(sub, ses)`` → list of destination paths copied.
    """
    results = {}
    for sub, ses in pairs:
        console.print(f"\n[bold cyan]sub-{sub}  ses-{ses}[/]")
        try:
            copied = copy_bold_for_subses(bidsdir, raw_dir, sub, ses)
            results[(sub, ses)] = copied
            console.print(
                f"  [green]✓[/] {len(copied)} file(s) copied → {op.join(raw_dir, f'sub-{sub}', f'ses-{ses}', 'func')}"
            )
        except Exception as exc:
            console.print(f"  [bold red][ERROR][/] {exc}")
            results[(sub, ses)] = []
    return results


# ---------------------------------------------------------------------------
# CLI (typer)
# ---------------------------------------------------------------------------


@app.command()
def run(
    bidsdir: Path = typer.Option(..., help="Path to the BIDS root directory."),
    raw_dir: Path = typer.Option(
        ..., "--raw-dir", help="Path to raw_nifti_sbref_fmap root directory."
    ),
    sub: Optional[str] = typer.Option(
        None, help="Subject label (without 'sub-' prefix)."
    ),
    ses: Optional[str] = typer.Option(
        None, help="Session label (without 'ses-' prefix)."
    ),
    subses_list: Optional[Path] = typer.Option(
        None, "--subses-list", help="TSV/CSV/TXT with columns 'sub' and 'ses'."
    ),
):
    """
    Copy *_bold* files from BIDS func to raw_nifti_sbref_fmap func.
    """
    if subses_list is not None:
        pairs = _parse_subses_list(subses_list)
        console.print(
            f"[cyan]Loaded {len(pairs)} sub/ses pair(s) from {subses_list}[/]"
        )
    elif sub is not None and ses is not None:
        pairs = [(sub, ses)]
    else:
        console.print("[bold red]Error:[/] provide --sub + --ses or --subses-list.")
        raise typer.Exit(code=1)

    results = batch_copy_bold(str(bidsdir), str(raw_dir), pairs)

    # Summary table
    table = Table(show_header=True, header_style="bold magenta", title="\nSummary")
    table.add_column("sub", style="bold")
    table.add_column("ses", style="bold")
    table.add_column("files copied", justify="right")

    for (s, e), files in results.items():
        count = len(files)
        color = "green" if count > 0 else "yellow"
        table.add_row(s, e, f"[{color}]{count}[/]")

    console.print(table)


if __name__ == "__main__":
    app()
