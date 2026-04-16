#!/usr/bin/env python3
# MIT License
# Copyright (c) 2024-2025 Yongning Lei
"""
Convert 3D-ME-EPI SWI DICOMs to BIDS NIfTI using dcm2niix.

Each acquisition produces 3 DICOM folders per phase-encoding direction
(sorted by trailing series number):
    index 0  raw magnitude      → part-mag
    index 1  normalised mag     → part-mag  (--include-norm, off by default)
    index 2  phase              → part-phase

Acquisition label is inferred from the folder name:
    4TEs + 416slc  →  acq-4TE38
    5TEs + 352slc  →  acq-5TE35

Phase-encoding direction:
    'iPE' or 'RPE' in folder name  →  dir-PA
    otherwise                       →  dir-AP

dcm2niix splits multi-echo volumes automatically into _e1, _e2 … output files.

Usage examples:
    python dcm2niix_swi.py -s 05 -e swi -d /path/to/dicoms -o /path/to/BIDS
    python dcm2niix_swi.py -s 05 -e swi -d /path/to/dicoms -o /path/to/BIDS --include-norm
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(add_completion=False)
console = Console()

# Accumulates (label, elapsed_seconds) for the timing summary
_timings: list[tuple[str, float]] = []


def setup_module(module_name: str) -> None:
    """Load an environment module and patch os.environ so all subprocesses inherit it."""
    result = subprocess.run(
        ["bash", "-lc", f"module load {module_name} 2>&1 && env"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        console.print(
            f"[yellow]Warning: module load {module_name} failed — "
            f"assuming it is already in PATH[/yellow]"
        )
        return
    for line in result.stdout.splitlines():
        if "=" in line:
            key, _, val = line.partition("=")
            os.environ[key] = val
    console.print(f"[dim]module load {module_name}  ✓[/dim]")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SERIES_RE = re.compile(r"3D-ME-EPI", re.IGNORECASE)
SERIES_NUM_RE = re.compile(r"_(\d+)$")

ACQ_RULES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"4TEs.*416slc", re.IGNORECASE), "4TE38"),
    (re.compile(r"5TEs.*352slc", re.IGNORECASE), "5TE38"),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def series_number(folder: Path) -> int:
    m = SERIES_NUM_RE.search(folder.name)
    return int(m.group(1)) if m else 0


def direction(folder: Path) -> str:
    up = folder.name.upper()
    return "PA" if ("IPE" in up or "RPE" in up) else "AP"


def acq_label(folder: Path) -> str:
    for pattern, label in ACQ_RULES:
        if pattern.search(folder.name):
            return f"acq-{label}"
    return "acq-SWI"


def run_dcm2niix(dcm_dir: Path, out_dir: Path, prefix: str) -> list[Path]:
    """Run dcm2niix and return sorted list of output .nii.gz files."""
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "dcm2niix",
        "-z",
        "y",  # gzip output
        "-b",
        "y",  # BIDS JSON sidecar
        "-f",
        prefix,  # output filename prefix
        "-o",
        str(out_dir),
        str(dcm_dir),
    ]
    console.print(f"    [dim]$ {' '.join(cmd)}[/dim]")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        console.print(f"    [red]dcm2niix failed:[/red]\n{result.stderr}")
        raise typer.Exit(1)
    return sorted(out_dir.glob(f"{prefix}*.nii.gz"))


def move_pair(src_nii: Path, dst_nii: Path) -> None:
    """Move NIfTI and its JSON sidecar to destination."""
    shutil.move(str(src_nii), str(dst_nii))
    stem = src_nii.name.removesuffix(".nii.gz")
    src_json = src_nii.parent / f"{stem}.json"
    if src_json.exists():
        dst_json = dst_nii.parent / f"{dst_nii.name.removesuffix('.nii.gz')}.json"
        shutil.move(str(src_json), str(dst_json))


def echo_label(nii_name: str) -> str:
    """Extract echo label from dcm2niix output filename (e.g. raw_e2.nii.gz → echo-2)."""
    m = re.search(r"_e(\d+)", nii_name)
    return f"echo-{m.group(1)}" if m else "echo-1"


def convert_folder(
    dcm_folder: Path,
    part: str,
    bids_prefix: str,
    acq: str,
    dir_label: str,
    run_label: str,
    swi_out: Path,
) -> None:
    """Run dcm2niix on one DICOM folder and rename outputs to BIDS."""
    label = f"{acq}_dir-{dir_label}_{run_label}_part-{part}"
    t0 = time.perf_counter()

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        niftis = run_dcm2niix(dcm_folder, tmp_path, "raw")

        if not niftis:
            console.print(
                f"    [yellow]Warning: no NIfTI output from {dcm_folder.name}[/yellow]"
            )
            _timings.append((label, time.perf_counter() - t0))
            return

        console.print(f"    {part}: {len(niftis)} echo file(s)")

        for nii in niftis:
            echo = echo_label(nii.name)
            out_name = f"{bids_prefix}_{acq}_dir-{dir_label}_{run_label}_{echo}_part-{part}.nii.gz"
            dst = swi_out / out_name
            move_pair(nii, dst)
            console.print(f"    [green]→[/green] {out_name}")

    _timings.append((label, time.perf_counter() - t0))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


@app.command()
def main(
    sub: str = typer.Option(..., "-s", help="Subject ID (without sub- prefix)"),
    ses: str = typer.Option(..., "-e", help="Session ID (without ses- prefix)"),
    dcm_dir: Path = typer.Option(
        ..., "-d", help="DICOM base dir containing 3D-ME-EPI subdirs"
    ),
    output_dir: Path = typer.Option(..., "-o", help="BIDS root output directory"),
    include_norm: bool = typer.Option(
        False,
        "--include-norm",
        help="Also convert normalised magnitude (folder index 1)",
    ),
) -> None:
    """Convert 3D-ME-EPI SWI DICOM series to BIDS NIfTI with dcm2niix."""

    setup_module("dcm2niix/2025")
    t_total = time.perf_counter()
    bids_prefix = f"sub-{sub}_ses-{ses}"
    swi_in = dcm_dir / f"sub-{sub}" / f"ses-{ses}"
    swi_out = output_dir / f"sub-{sub}" / f"ses-{ses}" / "anat"
    swi_out.mkdir(parents=True, exist_ok=True)

    # --- Discover series dirs ---
    swi_dirs = sorted(
        [d for d in swi_in.iterdir() if d.is_dir() and SERIES_RE.search(d.name)],
        key=series_number,
    )
    if not swi_dirs:
        console.print(f"[red]No 3D-ME-EPI directories found in {swi_in}[/red]")
        raise typer.Exit(1)

    console.print(f"[bold]Found {len(swi_dirs)} 3D-ME-EPI series:[/bold]")
    for d in swi_dirs:
        console.print(f"  {direction(d):2s}  {d.name}")

    # --- Split by direction and group into triplets ---
    ap_dirs = [d for d in swi_dirs if direction(d) == "AP"]
    pa_dirs = [d for d in swi_dirs if direction(d) == "PA"]

    def triplets(lst: list[Path]) -> list[list[Path]]:
        if len(lst) % 3 != 0:
            console.print(
                f"[yellow]Warning: {len(lst)} series not divisible by 3 "
                f"— incomplete runs may be skipped[/yellow]"
            )
        return [lst[i : i + 3] for i in range(0, len(lst) - 2, 3)]

    ap_runs = triplets(ap_dirs)
    pa_runs = triplets(pa_dirs)

    console.print(f"\nAP: {len(ap_runs)} run(s)   PA: {len(pa_runs)} run(s)")

    # --- Convert ---
    for dir_label, runs in (("AP", ap_runs), ("PA", pa_runs)):
        for run_idx, triplet in enumerate(runs, start=1):
            if len(triplet) < 3:
                console.print(
                    f"[yellow]Skipping incomplete triplet for dir-{dir_label} run-{run_idx:02d}[/yellow]"
                )
                continue

            acq = acq_label(triplet[0])
            run_label = f"run-{run_idx:02d}"

            console.rule(f"{bids_prefix}_{acq}_dir-{dir_label}_{run_label}")
            console.print(f"  [0] raw mag  : {triplet[0].name}")
            console.print(f"  [1] norm mag : {triplet[1].name}")
            console.print(f"  [2] phase    : {triplet[2].name}")

            # raw magnitude
            convert_folder(
                triplet[0], "mag", bids_prefix, acq, dir_label, run_label, swi_out
            )

            # normalised magnitude (optional)
            if include_norm:
                acq_norm = acq.replace("acq-", "acq-") + "norm"
                convert_folder(
                    triplet[1],
                    "mag",
                    bids_prefix,
                    acq_norm,
                    dir_label,
                    run_label,
                    swi_out,
                )
            else:
                console.print(
                    "    [dim]norm mag skipped (use --include-norm to convert)[/dim]"
                )

            # phase
            convert_folder(
                triplet[2], "phase", bids_prefix, acq, dir_label, run_label, swi_out
            )

    total_sec = time.perf_counter() - t_total

    # --- Timing summary ---
    console.rule("Timing summary")
    tbl = Table(show_lines=False, box=None)
    tbl.add_column("Step", style="cyan")
    tbl.add_column("Time (s)", justify="right")
    tbl.add_column("", justify="left", style="dim")

    for label, sec in _timings:
        bar = "█" * max(1, int(sec / max(s for _, s in _timings) * 20))
        tbl.add_row(label, f"{sec:.1f}", bar)

    tbl.add_row("", "", "")
    tbl.add_row("[bold]TOTAL[/bold]", f"[bold]{total_sec:.1f}[/bold]", "")
    console.print(tbl)

    console.print(f"\n[bold green]Done.[/bold green] Output: {swi_out}")


if __name__ == "__main__":
    app()
