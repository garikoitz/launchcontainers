"""
scan_acq_times.py
-----------------
Walk a BIDS dir, read AcquisitionTime from each .json sidecar,
and write a CSV: sub, ses, modality, nii_name, acq_time.

Usage
-----
    python scan_acq_times.py --bidsdir /path/to/BIDS --output acq_times.csv
    python scan_acq_times.py --bidsdir /path/to/BIDS --output acq_times.csv --workers 20
"""

import csv
import json
import glob
import os.path as op
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

console = Console()
app = typer.Typer(pretty_exceptions_show_locals=False)


def _read_one(nii: str) -> dict | None:
    parts = Path(nii).parts
    sub = next((p for p in parts if p.startswith("sub-")), None)
    ses = next((p for p in parts if p.startswith("ses-")), None)
    if not sub or not ses:
        return None

    # modality folder is the first directory directly under ses-*
    ses_idx = parts.index(ses)
    modality = parts[ses_idx + 1] if ses_idx + 1 < len(parts) - 1 else ""

    json_path = nii.replace(".nii.gz", ".json")
    acq_time = ""
    if op.exists(json_path):
        try:
            with open(json_path) as fh:
                acq_time = json.load(fh).get("AcquisitionTime", "")
        except Exception:
            acq_time = ""

    return {
        "sub": sub.replace("sub-", ""),
        "ses": ses.replace("ses-", ""),
        "modality": modality,
        "nii_name": Path(nii).name,
        "acq_time": acq_time,
    }


@app.command()
def main(
    bidsdir: Path = typer.Option(..., "--bidsdir", "-b"),
    output: Path = typer.Option(..., "--output", "-o"),
    workers: Optional[int] = typer.Option(
        None,
        "--workers",
        "-w",
        help="Number of parallel workers. Omit for serial mode.",
    ),
):
    pattern = op.join(str(bidsdir), "sub-*", "ses-*", "**", "*.nii.gz")
    nii_files = sorted(glob.glob(pattern, recursive=True))

    rows = []
    if workers:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        console.print(
            f"[dim]Found {len(nii_files)} .nii.gz files — reading with {workers} workers…[/]"
        )
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_read_one, nii): nii for nii in nii_files}
            for fut in as_completed(futures):
                result = fut.result()
                if result is not None:
                    rows.append(result)
    else:
        console.print(
            f"[dim]Found {len(nii_files)} .nii.gz files — reading serially…[/]"
        )
        for nii in nii_files:
            result = _read_one(nii)
            if result is not None:
                rows.append(result)

    rows.sort(key=lambda r: (r["sub"], r["ses"], r["nii_name"]))

    with open(output, "w", newline="") as fh:
        w = csv.DictWriter(
            fh, fieldnames=["sub", "ses", "modality", "nii_name", "acq_time"]
        )
        w.writeheader()
        w.writerows(rows)

    console.print(f"[bold green]{len(rows)} files written to {output}[/]")


if __name__ == "__main__":
    app()
