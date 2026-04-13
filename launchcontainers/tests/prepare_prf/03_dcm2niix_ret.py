#!/usr/bin/env python3
# MIT License
# Copyright (c) 2024-2025 Yongning Lei
"""
Convert retCB (and other ret*) DICOM series to BIDS NIfTI using dcm2niix.

Each run produces four DICOM folders:
    retCB_run01_40          raw magnitude  → task-retCB_run-01_bold
    retCB_run01_Pha_41      phase          → task-retCB_run-01_phase
    retCB_run01_SBRef_38    sbref mag      → task-retCB_run-01_sbref
    retCB_run01_SBRef_Pha_39  (skipped)

Classification by folder name:
    neither 'SBRef' nor 'Pha'  →  bold
    'Pha' only                  →  phase
    'SBRef' only                →  sbref
    both 'SBRef' and 'Pha'      →  skip

Output goes into:  BIDS/sub-{sub}/ses-{ses}/func/

Usage:
    python 03_dcm2niix_ret.py -s 01 -e 07 -d /path/dicom -o /path/BIDS
    python 03_dcm2niix_ret.py -s 01 -e 07 -d /path/dicom -o /path/BIDS --task retCB
    python 03_dcm2niix_ret.py -s 01 -e 07 -d /path/dicom -o /path/BIDS --dry-run
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import time
from collections import defaultdict
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(add_completion=False)
console = Console()

_timings: list[tuple[str, float]] = []

# ---------------------------------------------------------------------------
# Folder classification
# ---------------------------------------------------------------------------

_HAS_SBREF = re.compile(r"SBRef", re.IGNORECASE)
_HAS_PHA = re.compile(r"_Pha[_\d]|_Pha$", re.IGNORECASE)
_RUN_RE = re.compile(r"_run(\d+)", re.IGNORECASE)
_TASK_RE = re.compile(r"^([A-Za-z]+CB|[A-Za-z]+RW|[A-Za-z]+FF)")  # e.g. retCB, retRW


def classify(folder: Path) -> str | None:
    """Return 'bold', 'phase', 'sbref', or None (skip)."""
    name = folder.name
    has_sbref = bool(_HAS_SBREF.search(name))
    has_pha = bool(_HAS_PHA.search(name))
    if has_sbref and has_pha:
        return None  # sbref phase — not needed
    if has_sbref:
        return "sbref"
    if has_pha:
        return "phase"
    return "bold"


def run_number(folder: Path) -> int | None:
    m = _RUN_RE.search(folder.name)
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# Module loader (same pattern as 01_dcm2niix_swi.py)
# ---------------------------------------------------------------------------


def setup_module(module_name: str) -> None:
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
# Core conversion
# ---------------------------------------------------------------------------


def run_dcm2niix(dcm_dir: Path, out_dir: Path, prefix: str) -> list[Path]:
    """Run dcm2niix; return sorted list of produced .nii.gz files."""
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "dcm2niix",
        "-z",
        "y",
        "-b",
        "y",
        "-f",
        prefix,
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
    """Move NIfTI and its JSON sidecar to the destination."""
    shutil.move(str(src_nii), str(dst_nii))
    stem = src_nii.name.removesuffix(".nii.gz")
    src_json = src_nii.parent / f"{stem}.json"
    if src_json.exists():
        dst_json = dst_nii.parent / f"{dst_nii.name.removesuffix('.nii.gz')}.json"
        shutil.move(str(src_json), str(dst_json))


def convert_one(
    dcm_folder: Path,
    out_name: str,  # final BIDS stem (without .nii.gz)
    func_out: Path,
    dry_run: bool,
) -> None:
    """Run dcm2niix on *dcm_folder* and rename the result to *out_name*."""
    t0 = time.perf_counter()
    console.print(f"    [dim]source:[/dim] {dcm_folder.name}")
    console.print(f"    [dim]target:[/dim] {out_name}.nii.gz")

    if dry_run:
        _timings.append((out_name, 0.0))
        return

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        niftis = run_dcm2niix(dcm_folder, tmp_path, "raw")

        if not niftis:
            console.print(
                f"    [yellow]Warning: no NIfTI output from {dcm_folder.name}[/yellow]"
            )
            _timings.append((out_name, time.perf_counter() - t0))
            return

        if len(niftis) > 1:
            console.print(
                f"    [yellow]Warning: {len(niftis)} NIfTIs found — using first[/yellow]"
            )
        nii = niftis[0]
        dst = func_out / f"{out_name}.nii.gz"
        move_pair(nii, dst)
        console.print(f"    [green]→[/green] {dst.name}")

    _timings.append((out_name, time.perf_counter() - t0))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@app.command()
def main(
    sub: str = typer.Option(..., "-s", help="Subject ID (without sub- prefix)"),
    ses: str = typer.Option(..., "-e", help="Session ID (without ses- prefix)"),
    dcm_dir: Path = typer.Option(
        ..., "-d", help="DICOM base directory (containing sub-XX/ses-XX/)"
    ),
    output_dir: Path = typer.Option(..., "-o", help="BIDS root output directory"),
    task: str = typer.Option(
        "retCB", "--task", help="Task label to convert (default: retCB)"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print plan without running dcm2niix"
    ),
) -> None:
    """Convert ret* DICOM bold/phase/sbref series to BIDS NIfTI."""

    setup_module("dcm2niix/2025")
    t_total = time.perf_counter()

    bids_prefix = f"sub-{sub}_ses-{ses}"
    func_out = output_dir / f"sub-{sub}" / f"ses-{ses}" / "func"
    ses_dcm = dcm_dir / f"sub-{sub}" / f"ses-{ses}"

    if not ses_dcm.exists():
        console.print(f"[red]DICOM session dir not found:[/red] {ses_dcm}")
        raise typer.Exit(1)

    func_out.mkdir(parents=True, exist_ok=True)

    # ── Discover folders ──────────────────────────────────────────────────────
    task_re = re.compile(rf"^{re.escape(task)}_", re.IGNORECASE)
    folders = sorted(
        [d for d in ses_dcm.iterdir() if d.is_dir() and task_re.match(d.name)],
        key=lambda d: d.name,
    )

    if not folders:
        console.print(f"[red]No {task}* directories found in {ses_dcm}[/red]")
        raise typer.Exit(1)

    # ── Group by run number ───────────────────────────────────────────────────
    by_run: dict[int, dict[str, Path]] = defaultdict(dict)
    skipped: list[str] = []

    for folder in folders:
        rn = run_number(folder)
        if rn is None:
            console.print(
                f"[yellow]Cannot parse run number from {folder.name} — skipping[/yellow]"
            )
            continue
        suffix = classify(folder)
        if suffix is None:
            skipped.append(folder.name)
            continue
        by_run[rn][suffix] = folder

    # ── Preview table ─────────────────────────────────────────────────────────
    mode_tag = (
        "[bold yellow]DRY-RUN[/bold yellow]"
        if dry_run
        else "[bold red]EXECUTE[/bold red]"
    )
    console.rule(f"sub-{sub}  ses-{ses}  task-{task}  {mode_tag}")

    tbl = Table(show_lines=False, box=None)
    tbl.add_column("run", style="cyan", width=6)
    tbl.add_column("bold", width=40)
    tbl.add_column("phase", width=40)
    tbl.add_column("sbref", width=40)

    for rn in sorted(by_run):
        grp = by_run[rn]
        tbl.add_row(
            f"{rn:02d}",
            grp.get("bold", Path("")).name or "[red]MISSING[/red]",
            grp.get("phase", Path("")).name or "[red]MISSING[/red]",
            grp.get("sbref", Path("")).name or "[red]MISSING[/red]",
        )
    console.print(tbl)

    if skipped:
        console.print(f"[dim]Skipped (SBRef+Pha): {', '.join(skipped)}[/dim]")

    # ── Convert ───────────────────────────────────────────────────────────────
    for rn in sorted(by_run):
        grp = by_run[rn]
        run_label = f"run-{rn:02d}"
        console.rule(f"{bids_prefix}  task-{task}  {run_label}")

        for suffix in ("bold", "phase", "sbref"):
            if suffix not in grp:
                console.print(
                    f"  [yellow]{suffix} folder not found — skipping[/yellow]"
                )
                continue
            out_name = f"{bids_prefix}_task-{task}_{run_label}_{suffix}"
            convert_one(grp[suffix], out_name, func_out, dry_run)

    # ── Timing summary ────────────────────────────────────────────────────────
    total_elapsed = time.perf_counter() - t_total
    console.rule("Timing")
    tbl2 = Table(show_lines=False, box=None)
    tbl2.add_column("output", style="cyan")
    tbl2.add_column("sec", justify="right")
    for label, t in _timings:
        tbl2.add_row(label, f"{t:.1f}")
    tbl2.add_row("[bold]TOTAL[/bold]", f"[bold]{total_elapsed:.1f}[/bold]")
    console.print(tbl2)


if __name__ == "__main__":
    app()
