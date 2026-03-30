"""
restore_sbref.py
----------------
For each sub/ses:

1. Remove all ``*sbref*`` files (``.nii.gz`` and ``.json``) from
   ``<bidsdir>/sub-{sub}/ses-{ses}/func/``
2. Copy ``*sbref*`` files from
   ``<raw_dir>/sub-{sub}/ses-{ses}/func/`` back into the BIDS func dir.

Batch mode runs all pairs **in parallel** using a thread pool.

CLI usage
---------
Single pair::

    python restore_sbref.py --bidsdir /path/to/BIDS \\
        --raw-dir /path/to/raw_nifti_sbref_fmap --sub 11 --ses 03

Batch (parallel)::

    python restore_sbref.py --bidsdir /path/to/BIDS \\
        --raw-dir /path/to/raw_nifti_sbref_fmap --subses-list subses.txt

    # control parallelism (default: 4 workers)
    python restore_sbref.py ... --subses-list subses.txt --workers 8
"""

from __future__ import annotations

import csv
import glob
import os
import os.path as op
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
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
# Core function (single sub/ses)
# ---------------------------------------------------------------------------


def restore_sbref_for_subses(
    bidsdir: str,
    raw_dir: str,
    sub: str,
    ses: str,
) -> dict:
    """
    Remove sbref files from BIDS func and restore them from raw_nifti_sbref_fmap.

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
    dict
        Keys: ``sub``, ``ses``, ``removed``, ``copied``, ``error``.
    """
    result = {"sub": sub, "ses": ses, "removed": 0, "copied": 0, "error": None}

    bids_func = op.join(bidsdir, f"sub-{sub}", f"ses-{ses}", "func")
    raw_func = op.join(raw_dir, f"sub-{sub}", f"ses-{ses}", "func")

    try:
        # ------------------------------------------------------------------
        # 1. Remove existing sbrefs from BIDS func
        # ------------------------------------------------------------------
        existing = sorted(glob.glob(op.join(bids_func, "*sbref*")))
        existing = [f for f in existing if f.endswith(".nii.gz") or f.endswith(".json")]

        for f in existing:
            os.remove(f)
            result["removed"] += 1

        # ------------------------------------------------------------------
        # 2. Copy sbrefs from raw back to BIDS func
        # ------------------------------------------------------------------
        sources = sorted(glob.glob(op.join(raw_func, "*sbref*")))
        sources = [f for f in sources if f.endswith(".nii.gz") or f.endswith(".json")]

        if not sources:
            result["error"] = f"No *sbref* files found in {raw_func}"
            return result

        os.makedirs(bids_func, exist_ok=True)
        for src in sources:
            shutil.copy2(src, op.join(bids_func, op.basename(src)))
            result["copied"] += 1

    except Exception as exc:
        result["error"] = str(exc)

    return result


# ---------------------------------------------------------------------------
# Batch function (parallel)
# ---------------------------------------------------------------------------


def batch_restore_sbref(
    bidsdir: str,
    raw_dir: str,
    pairs: list[tuple[str, str]],
    workers: int = 4,
) -> list[dict]:
    """
    Restore sbref files for multiple sub/ses pairs in parallel.

    Parameters
    ----------
    bidsdir, raw_dir : str
        BIDS root and raw_nifti_sbref_fmap root directories.
    pairs : list of (sub, ses)
        Subject/session pairs to process.
    workers : int
        Number of parallel threads.

    Returns
    -------
    list[dict]
        One result dict per pair (see :func:`restore_sbref_for_subses`).
    """
    results = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(restore_sbref_for_subses, bidsdir, raw_dir, sub, ses): (
                sub,
                ses,
            )
            for sub, ses in pairs
        }
        for future in as_completed(futures):
            sub, ses = futures[future]
            try:
                res = future.result()
            except Exception as exc:
                res = {
                    "sub": sub,
                    "ses": ses,
                    "removed": 0,
                    "copied": 0,
                    "error": str(exc),
                }
            results.append(res)

            # live progress
            if res["error"]:
                console.print(
                    f"  [bold red][ERROR][/] sub-{sub} ses-{ses}: {res['error']}"
                )
            else:
                console.print(
                    f"  [green]✓[/] sub-{sub} ses-{ses}  "
                    f"removed={res['removed']}  copied={res['copied']}"
                )

    # return sorted for a stable summary table
    results.sort(key=lambda r: (r["sub"], r["ses"]))
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
    workers: int = typer.Option(
        4, "--workers", help="Number of parallel workers (batch mode only)."
    ),
):
    """
    Remove BIDS sbref files and restore them from raw_nifti_sbref_fmap.
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

    console.print(
        f"\n[bold]bidsdir :[/] {bidsdir}\n"
        f"[bold]raw_dir :[/] {raw_dir}\n"
        f"[bold]workers :[/] {workers if len(pairs) > 1 else 1} "
        f"({'parallel' if len(pairs) > 1 else 'single'})\n"
    )

    results = batch_restore_sbref(str(bidsdir), str(raw_dir), pairs, workers=workers)

    # Summary table
    table = Table(show_header=True, header_style="bold magenta", title="Summary")
    table.add_column("sub", style="bold")
    table.add_column("ses", style="bold")
    table.add_column("removed", justify="right")
    table.add_column("copied", justify="right")
    table.add_column("status")

    for r in results:
        if r["error"]:
            status = f"[red]{r['error']}[/]"
        else:
            status = "[green]OK[/]"
        table.add_row(
            r["sub"],
            r["ses"],
            str(r["removed"]),
            f"[green]{r['copied']}[/]" if r["copied"] > 0 else "[yellow]0[/]",
            status,
        )

    console.print(table)


if __name__ == "__main__":
    app()
