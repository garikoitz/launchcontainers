"""
01b_screening_vistadisplog.py
------------------------------
Walk BIDS/sourcedata/vistadisplog/sub-*/ses-* and read *_params.mat files.
For each file, resolve the symlink target (the original timestamp .mat log).

Output CSV columns match 01_screening_dataset_based_on_acqtime.py:
    sub, ses, modality, nii_name, acq_time

Where:
    modality  = "retlog"
    nii_name  = the *_params.mat filename (BIDS-named)
    acq_time  = the filename it is linked to (symlink target basename)

This makes the output directly comparable with compare_bcbl_dipc.py and
summarise_bcbl_dipc_file_mismatch.py.

Usage
-----
    python 01b_screening_vistadisplog.py --bidsdir /path/to/BIDS --output retlog.csv
"""

import csv
import glob
import os
import os.path as op
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

console = Console()
app = typer.Typer(pretty_exceptions_show_locals=False)


def _read_one(mat_path: str) -> dict | None:
    parts = Path(mat_path).parts
    sub = next((p for p in parts if p.startswith("sub-")), None)
    ses = next((p for p in parts if p.startswith("ses-")), None)
    if not sub or not ses:
        return None

    nii_name = Path(mat_path).name

    # Resolve symlink target — the linked-to filename is stored as acq_time
    linked_to = ""
    try:
        if os.path.islink(mat_path):
            linked_to = Path(os.readlink(mat_path)).name
        else:
            # Not a symlink: try to read the internal params field as fallback
            try:
                from scipy.io import loadmat

                data = loadmat(mat_path, simplify_cells=True)
                params = data.get("params", {})
                # try common field names that store the linked log file
                for field in (
                    "dataFile",
                    "logFile",
                    "fileName",
                    "loadMatrix",
                    "dataFileName",
                ):
                    val = params.get(field, "") if isinstance(params, dict) else ""
                    if val:
                        linked_to = Path(str(val)).name
                        break
            except Exception:
                linked_to = ""
    except Exception:
        linked_to = ""

    return {
        "sub": sub.replace("sub-", ""),
        "ses": ses.replace("ses-", ""),
        "modality": "retlog",
        "nii_name": nii_name,
        "acq_time": linked_to,
    }


@app.command()
def main(
    bidsdir: Path = typer.Option(..., "--bidsdir", "-b", help="BIDS root directory."),
    output: Path = typer.Option(..., "--output", "-o", help="Output CSV path."),
    workers: Optional[int] = typer.Option(
        None, "--workers", "-w", help="Parallel workers. Omit for serial."
    ),
):
    """Screen vistadisplog *_params.mat files and record their symlink targets."""
    pattern = op.join(
        str(bidsdir), "sourcedata", "vistadisplog", "sub-*", "ses-*", "*_params.mat"
    )
    mat_files = sorted(glob.glob(pattern))
    console.print(f"[dim]Found {len(mat_files)} *_params.mat files[/]")

    rows = []
    if workers:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        console.print(f"[dim]Reading with {workers} workers…[/]")
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_read_one, f): f for f in mat_files}
            for fut in as_completed(futures):
                result = fut.result()
                if result is not None:
                    rows.append(result)
    else:
        for mat in mat_files:
            result = _read_one(mat)
            if result is not None:
                rows.append(result)

    rows.sort(key=lambda r: (r["sub"], r["ses"], r["nii_name"]))

    with open(output, "w", newline="") as fh:
        w = csv.DictWriter(
            fh, fieldnames=["sub", "ses", "modality", "nii_name", "acq_time"]
        )
        w.writeheader()
        w.writerows(rows)

    console.print(f"[bold green]{len(rows)} rows written to {output}[/]")


if __name__ == "__main__":
    app()
