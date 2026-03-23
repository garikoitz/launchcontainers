"""
check_bold_tasks.py
-------------------
Diagnostic: for each sub/ses, count unique task labels found in bold filenames
across the BIDS and fMRIprep func directories.

Output TSV columns::

    sub | ses | bids_n_tasks | bids_tasks | fmriprep_n_tasks | fmriprep_tasks

CLI usage
---------
Single pair::

    python check_bold_tasks.py --bidsdir /path/to/BIDS \\
        --fmriprep-dir /path/to/fmriprep --sub 11 --ses 03

Batch via subses list (TSV/CSV/TXT)::

    python check_bold_tasks.py --bidsdir /path/to/BIDS \\
        --fmriprep-dir /path/to/fmriprep --subses-list subses.tsv

Import usage
------------
    from launchcontainers.tests.check_bold_tasks import check_bold_tasks
    check_bold_tasks("/path/to/BIDS", "/path/to/fmriprep", sub="11", ses="03")
"""
from __future__ import annotations

import csv
import glob
import os.path as op
import re
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

console = Console()
app = typer.Typer(pretty_exceptions_show_locals=False)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_subses_list(path: Path) -> list[tuple[str, str]]:
    """Read a TSV, CSV, or TXT file with columns ``sub`` and ``ses``.

    Delimiter is inferred from the file extension (``.tsv`` → tab,
    ``.csv`` / ``.txt`` → comma).  Falls back to sniffer if the extension
    is unrecognised.
    """
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
            pairs.append((str(row["sub"]).strip(), str(row["ses"]).strip()))
    return pairs


def _unique_tasks_in_dir(func_dir: str, sub: str, ses: str) -> list[str]:
    """Return sorted unique task labels from bold filenames in *func_dir*."""
    bold_files = glob.glob(
        op.join(func_dir, f"sub-{sub}_ses-{ses}_task-*_*bold*")
    )
    tasks = set()
    for f in bold_files:
        m = re.search(r"task-(\w+)", op.basename(f))
        if m:
            tasks.add(m.group(1))
    return sorted(tasks)


# ---------------------------------------------------------------------------
# Core function
# ---------------------------------------------------------------------------


def check_bold_tasks(
    bidsdir: str,
    fmriprep_dir: str,
    sub: str,
    ses: str,
    output_dir: str | None = None,
) -> dict:
    """
    Count unique task labels in BIDS and fMRIprep func directories.

    Parameters
    ----------
    bidsdir : str
        Absolute path to the BIDS root directory.
    fmriprep_dir : str
        Absolute path to the fMRIprep derivatives directory.
    sub : str
        Subject label without the ``sub-`` prefix.
    ses : str
        Session label without the ``ses-`` prefix.
    output_dir : str or None
        Where to append the output TSV row.  Defaults to
        ``<bidsdir>/sourcedata/qc/``.

    Returns
    -------
    dict
        Keys: ``sub``, ``ses``, ``bids_n_tasks``, ``bids_tasks``,
        ``fmriprep_n_tasks``, ``fmriprep_tasks``.
    """
    bids_func = op.join(bidsdir, f"sub-{sub}", f"ses-{ses}", "func")
    fmriprep_func = op.join(fmriprep_dir, f"sub-{sub}", f"ses-{ses}", "func")

    bids_tasks = _unique_tasks_in_dir(bids_func, sub, ses)
    fmriprep_tasks = _unique_tasks_in_dir(fmriprep_func, sub, ses)

    return {
        "sub": sub,
        "ses": ses,
        "bids_n_tasks": len(bids_tasks),
        "bids_tasks": ",".join(bids_tasks) if bids_tasks else "N/A",
        "fmriprep_n_tasks": len(fmriprep_tasks),
        "fmriprep_tasks": ",".join(fmriprep_tasks) if fmriprep_tasks else "N/A",
    }


# ---------------------------------------------------------------------------
# CLI (typer)
# ---------------------------------------------------------------------------


@app.command()
def run(
    bidsdir: Path = typer.Option(..., help="Path to the BIDS root directory."),
    fmriprep_dir: Path = typer.Option(..., "--fmriprep-dir", help="Path to the fMRIprep derivatives directory."),
    sub: Optional[str] = typer.Option(None, help="Subject label (without 'sub-' prefix)."),
    ses: Optional[str] = typer.Option(None, help="Session label (without 'ses-' prefix)."),
    subses_list: Optional[Path] = typer.Option(
        None, "--subses-list", help="TSV/CSV/TXT with columns 'sub' and 'ses' for batch mode."
    ),
    output_dir: Optional[Path] = typer.Option(
        None, help="Directory for the output TSV. Defaults to <bidsdir>/sourcedata/qc/."
    ),
):
    """
    Count unique task labels in BIDS and fMRIprep bold files per sub/ses.
    """
    if subses_list is not None:
        pairs = _parse_subses_list(subses_list)
        console.print(f"[cyan]Loaded {len(pairs)} sub/ses pair(s) from {subses_list}[/]")
    elif sub is not None and ses is not None:
        pairs = [(sub, ses)]
    else:
        console.print("[bold red]Error:[/] provide --sub + --ses or --subses-list.")
        raise typer.Exit(code=1)

    out_dir = str(output_dir) if output_dir is not None else op.join(str(bidsdir), "sourcedata", "qc")
    import os
    os.makedirs(out_dir, exist_ok=True)
    out_file = op.join(out_dir, "bold_task_summary.tsv")

    fieldnames = ["sub", "ses", "bids_n_tasks", "bids_tasks", "fmriprep_n_tasks", "fmriprep_tasks"]
    write_header = not op.exists(out_file)

    rows = []
    for s, e in pairs:
        try:
            row = check_bold_tasks(str(bidsdir), str(fmriprep_dir), sub=s, ses=e, output_dir=out_dir)
            rows.append(row)
        except Exception as exc:
            console.print(f"  [yellow][SKIP][/] sub-{s} ses-{e}: {exc}")

    # Write TSV
    with open(out_file, "a", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter="\t")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

    console.print(f"\n[bold cyan]Output →[/] [dim]{out_file}[/]\n")

    # Rich summary table
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("sub", style="bold")
    table.add_column("ses", style="bold")
    table.add_column("bids_n_tasks", justify="center")
    table.add_column("bids_tasks", style="dim")
    table.add_column("fmriprep_n_tasks", justify="center")
    table.add_column("fmriprep_tasks", style="dim")

    for row in rows:
        bids_col = (
            f"[green]{row['bids_n_tasks']}[/]" if row["bids_n_tasks"] > 0 else "[yellow]0[/]"
        )
        fp_col = (
            f"[green]{row['fmriprep_n_tasks']}[/]" if row["fmriprep_n_tasks"] > 0 else "[yellow]0[/]"
        )
        table.add_row(
            row["sub"],
            row["ses"],
            bids_col,
            row["bids_tasks"],
            fp_col,
            row["fmriprep_tasks"],
        )

    console.print(table)


if __name__ == "__main__":
    app()
