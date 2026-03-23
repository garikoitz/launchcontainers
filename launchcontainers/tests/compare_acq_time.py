"""
compare_acq_time.py
-------------------
Diagnostic: compare acquisition times from vistadisplog .mat filenames with
AcquisitionTime in BIDS bold JSON sidecars for one or more sub/ses pairs.

Output TSV columns::

    row | log_file_name | stim_name | mat_acq_time
        | bold_json | task_run | json_acq_time | diff_sec

``diff_sec`` = json_acq_time − mat_acq_time.

CLI usage
---------
Single pair::

    python compare_acq_time.py --bidsdir /path/to/BIDS --sub 11 --ses 03

Multiple pairs via TSV (columns: sub, ses)::

    python compare_acq_time.py --bidsdir /path/to/BIDS --subses-list subses.tsv

Import usage
------------
    from launchcontainers.tests.compare_acq_time import compare_acq_time
    compare_acq_time("/path/to/BIDS", sub="11", ses="03")
"""
from __future__ import annotations

import csv
import glob
import json
import os
import os.path as op
import re
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

console = Console()
app = typer.Typer(pretty_exceptions_show_locals=False)

from launchcontainers.utils import parse_hms


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _hms_to_seconds(hms: str) -> float:
    """Convert ``HH:MM:SS[.f]`` to total seconds; returns NaN on failure."""
    try:
        h, m, s = hms.split(":")
        return int(h) * 3600 + int(m) * 60 + float(s)
    except Exception:
        return float("nan")


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


# ---------------------------------------------------------------------------
# Core function
# ---------------------------------------------------------------------------


def compare_acq_time(
    bidsdir: str,
    sub: str,
    ses: str,
    output_dir: str | None = None,
) -> str:
    """
    Compare vistadisplog .mat timestamps with BIDS bold JSON AcquisitionTime.

    Parameters
    ----------
    bidsdir : str
        Absolute path to the BIDS root directory.
    sub : str
        Subject label without the ``sub-`` prefix.
    ses : str
        Session label without the ``ses-`` prefix.
    output_dir : str or None
        Where to write the output TSV.  Defaults to the vistadisplog session
        directory (``<bidsdir>/sourcedata/vistadisplog/sub-X/ses-X/``).

    Returns
    -------
    str
        Absolute path to the output TSV.
    """
    from scipy.io import loadmat

    vistadisplog_dir = op.join(
        bidsdir, "sourcedata", "vistadisplog", f"sub-{sub}", f"ses-{ses}"
    )

    # ------------------------------------------------------------------
    # 1. Mat side — acq_time from filename, stim_name from params
    # ------------------------------------------------------------------
    mat_files = sorted(glob.glob(op.join(vistadisplog_dir, "20*.mat")))
    if not mat_files:
        raise FileNotFoundError(f"No '20*.mat' files found in {vistadisplog_dir}")

    mat_rows = []
    for mat_file in mat_files:
        log_name = op.basename(mat_file)
        acq_time = parse_hms(op.splitext(log_name)[0])
        try:
            params = loadmat(mat_file, simplify_cells=True)["params"]
            stim_basename = op.basename(params["loadMatrix"])
        except Exception:
            stim_basename = "unknown"
        mat_rows.append(
            {"log_file_name": log_name, "stim_name": stim_basename, "mat_acq_time": acq_time}
        )

    mat_rows.sort(key=lambda r: _hms_to_seconds(r["mat_acq_time"]))

    # ------------------------------------------------------------------
    # 2. JSON side — AcquisitionTime from non-fLoc bold sidecars
    # ------------------------------------------------------------------
    func_dir = op.join(bidsdir, f"sub-{sub}", f"ses-{ses}", "func")
    json_files = sorted(
        glob.glob(op.join(func_dir, f"sub-{sub}_ses-{ses}_task-*_*bold.json"))
    )
    json_files = [f for f in json_files if "task-fLoc" not in op.basename(f)]

    json_rows = []
    for json_file in json_files:
        basename = op.basename(json_file)
        with open(json_file) as fh:
            metadata = json.load(fh)
        raw_acq = metadata.get("AcquisitionTime", "")
        acq_time = parse_hms(raw_acq) if raw_acq else "unknown"
        m = re.search(r"task-\w+_run-\d+", basename)
        task_run = m.group(0) if m else basename
        json_rows.append(
            {"bold_json": basename, "task_run": task_run, "json_acq_time": acq_time}
        )

    json_rows.sort(key=lambda r: _hms_to_seconds(r["json_acq_time"]))

    # ------------------------------------------------------------------
    # 3. Merge row-by-row and compute diff
    # ------------------------------------------------------------------
    n = max(len(mat_rows), len(json_rows))
    merged = []
    for i in range(n):
        mat = mat_rows[i] if i < len(mat_rows) else {}
        jsn = json_rows[i] if i < len(json_rows) else {}
        mat_sec = _hms_to_seconds(mat.get("mat_acq_time", ""))
        jsn_sec = _hms_to_seconds(jsn.get("json_acq_time", ""))
        diff_sec = (
            round(jsn_sec - mat_sec, 2)
            if (mat_sec == mat_sec and jsn_sec == jsn_sec)
            else "N/A"
        )
        merged.append(
            {
                "row": i + 1,
                "log_file_name": mat.get("log_file_name", "N/A"),
                "stim_name": mat.get("stim_name", "N/A"),
                "mat_acq_time": mat.get("mat_acq_time", "N/A"),
                "bold_json": jsn.get("bold_json", "N/A"),
                "task_run": jsn.get("task_run", "N/A"),
                "json_acq_time": jsn.get("json_acq_time", "N/A"),
                "diff_sec": diff_sec,
            }
        )

    # ------------------------------------------------------------------
    # 4. Write output TSV
    # ------------------------------------------------------------------
    if output_dir is None:
        output_dir = vistadisplog_dir
    os.makedirs(output_dir, exist_ok=True)

    out_file = op.join(output_dir, f"sub-{sub}_ses-{ses}_desc-acqtime_compare.tsv")
    fieldnames = [
        "row", "log_file_name", "stim_name", "mat_acq_time",
        "bold_json", "task_run", "json_acq_time", "diff_sec",
    ]
    with open(out_file, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(merged)

    # ------------------------------------------------------------------
    # 5. Rich summary table
    # ------------------------------------------------------------------
    console.print(f"\n[bold cyan]sub-{sub}  ses-{ses}[/]  →  [dim]{out_file}[/]")

    if len(mat_rows) != len(json_rows):
        console.print(
            f"  [bold yellow][WARNING][/] mat count ({len(mat_rows)}) "
            f"≠ JSON count ({len(json_rows)})"
        )

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("#", justify="right", style="dim")
    table.add_column("mat_acq_time", justify="center")
    table.add_column("json_acq_time", justify="center")
    table.add_column("diff_sec", justify="right")
    table.add_column("stim_name", style="dim")
    table.add_column("task_run", style="dim")

    for row in merged:
        diff = row["diff_sec"]
        diff_style = "green" if diff != "N/A" and abs(diff) <= 5 else "yellow"
        table.add_row(
            str(row["row"]),
            row["mat_acq_time"],
            row["json_acq_time"],
            f"[{diff_style}]{diff}[/]",
            row["stim_name"],
            row["task_run"],
        )

    console.print(table)
    return out_file


# ---------------------------------------------------------------------------
# CLI (typer)
# ---------------------------------------------------------------------------


@app.command()
def run(
    bidsdir: Path = typer.Option(..., help="Path to the BIDS root directory."),
    sub: Optional[str] = typer.Option(None, help="Subject label (without 'sub-' prefix)."),
    ses: Optional[str] = typer.Option(None, help="Session label (without 'ses-' prefix)."),
    subses_list: Optional[Path] = typer.Option(
        None, "--subses-list", help="TSV/CSV with columns 'sub' and 'ses' for batch mode."
    ),
    output_dir: Optional[Path] = typer.Option(
        None, help="Output directory for TSV(s). Defaults to the vistadisplog session dir."
    ),
):
    """
    Compare vistadisplog .mat acquisition times with BIDS bold JSON AcquisitionTime.
    """
    if subses_list is not None:
        pairs = _parse_subses_list(subses_list)
        console.print(f"[cyan]Loaded {len(pairs)} sub/ses pair(s) from {subses_list}[/]")
    elif sub is not None and ses is not None:
        pairs = [(sub, ses)]
    else:
        console.print("[bold red]Error:[/] provide --sub + --ses or --subses-list.")
        raise typer.Exit(code=1)

    out_dir = str(output_dir) if output_dir is not None else None
    for s, e in pairs:
        try:
            compare_acq_time(str(bidsdir), sub=s, ses=e, output_dir=out_dir)
        except FileNotFoundError as exc:
            console.print(f"  [yellow][SKIP][/] sub-{s} ses-{e}: {exc}")


if __name__ == "__main__":
    app()
