#!/usr/bin/env python3
# MIT License
# Copyright (c) 2024-2025 Yongning Lei
"""
Average part-mag SWI files at three levels of aggregation (all in parallel).

Input files:
    sub-{sub}_ses-{ses}_acq-{acq}_dir-{dir}_run-{run}_echo-{echo}_part-mag.nii.gz

Three averages produced per acq label:

  1. desc-avgechos   average echoes only, dir and run kept separate
       sub-{sub}_ses-{ses}_acq-{acq}_dir-{dir}_{run}_desc-avgechos_part-mag.nii.gz

  2. desc-avgrun     average echoes + both dirs (AP & PA), run kept separate
       sub-{sub}_ses-{ses}_acq-{acq}_{run}_desc-avgrun_part-mag.nii.gz

  3. desc-avgall     average everything (echoes, dirs, runs)
       sub-{sub}_ses-{ses}_acq-{acq}_desc-avgall_part-mag.nii.gz

All averages use:  fslmaths img1 -add img2 ... -div N output

Usage:
    python 03_avg_swi_mag.py -s 05 -e swi -d /path/to/BIDS
    python 03_avg_swi_mag.py -f subseslist.txt -d /path/to/BIDS
    python 03_avg_swi_mag.py -s 05 -e swi -d /path/to/BIDS --workers 6 --dry-run
"""

from __future__ import annotations

import os
import re
import subprocess
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(add_completion=False)
console = Console()

FILE_RE = re.compile(
    r"^sub-(?P<sub>[^_]+)_ses-(?P<ses>[^_]+)_acq-(?P<acq>[^_]+)"
    r"_dir-(?P<dir>[^_]+)_run-(?P<run>[^_]+)_echo-(?P<echo>[^_]+)_part-mag\.nii\.gz$"
)


# ---------------------------------------------------------------------------
# Module loader
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
# Core averaging function (one worker per job)
# ---------------------------------------------------------------------------


def fslmaths_avg(files: list[Path], out: Path, dry_run: bool) -> tuple[str, bool, str]:
    """
    fslmaths img1 -add img2 ... -div N out
    Returns (out.name, success, error_msg).
    """
    n = len(files)
    lbl = out.name.removesuffix(".nii.gz")

    if dry_run:
        return lbl, True, f"{n} files ÷ {n}"

    cmd = ["fslmaths", str(files[0])]
    for f in files[1:]:
        cmd += ["-add", str(f)]
    cmd += ["-div", str(n), str(out)]

    try:
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            return lbl, False, r.stderr.strip()
        return lbl, True, ""
    except Exception as e:
        return lbl, False, str(e)


# ---------------------------------------------------------------------------
# Per-subject processing
# ---------------------------------------------------------------------------


def process_subject(
    sub: str, ses: str, bids_dir: Path, workers: int, dry_run: bool
) -> None:
    anat_dir = bids_dir / f"sub-{sub}" / f"ses-{ses}" / "anat"
    if not anat_dir.exists():
        console.print(f"[red]Not found:[/red] {anat_dir}")
        return

    # --- Collect echo files (skip existing averages) ---
    echo_files: list[Path] = []
    for nii in sorted(anat_dir.glob("*_part-mag.nii.gz")):
        m = FILE_RE.match(nii.name)
        if not m:
            continue
        if m.group("sub") != sub or m.group("ses") != ses:
            continue
        if "desc-" in nii.name:
            continue
        echo_files.append(nii)

    if not echo_files:
        console.print(f"[yellow]No part-mag echo files in {anat_dir}[/yellow]")
        return

    # --- Build grouping dicts ---
    # key → list of files
    g_avgechos: dict[tuple, list[Path]] = defaultdict(list)  # (acq, dir, run)
    g_avgrun: dict[tuple, list[Path]] = defaultdict(list)  # (acq, run)
    g_avgall: dict[tuple, list[Path]] = defaultdict(list)  # (acq,)

    for nii in echo_files:
        m = FILE_RE.match(nii.name)
        acq, dir_, run = m.group("acq"), m.group("dir"), m.group("run")
        g_avgechos[(acq, dir_, run)].append(nii)
        g_avgrun[(acq, run)].append(nii)
        g_avgall[(acq,)].append(nii)

    # --- Build job list: (files, out_path, level_label) ---
    jobs: list[tuple[list[Path], Path, str]] = []

    for (acq, dir_, run), files in sorted(g_avgechos.items()):
        out = (
            anat_dir
            / f"sub-{sub}_ses-{ses}_acq-{acq}_dir-{dir_}_{run}_desc-avgechos_part-mag.nii.gz"
        )
        jobs.append((sorted(files), out, "avgechos"))

    for (acq, run), files in sorted(g_avgrun.items()):
        out = (
            anat_dir
            / f"sub-{sub}_ses-{ses}_acq-{acq}_{run}_desc-avgrun_part-mag.nii.gz"
        )
        jobs.append((sorted(files), out, "avgrun"))

    for (acq,), files in sorted(g_avgall.items()):
        out = anat_dir / f"sub-{sub}_ses-{ses}_acq-{acq}_desc-avgall_part-mag.nii.gz"
        jobs.append((sorted(files), out, "avgall"))

    # --- Preview table ---
    console.rule(f"sub-{sub}  ses-{ses}  ({len(jobs)} jobs)")

    tbl = Table(show_lines=False, box=None)
    tbl.add_column("level", style="cyan", width=10)
    tbl.add_column("n files", justify="right", width=8)
    tbl.add_column("output")

    for files, out, level in jobs:
        tbl.add_row(level, str(len(files)), out.name)

    console.print(tbl)
    console.print(f"\nRunning with up to [bold]{workers}[/bold] parallel worker(s)...")

    # --- Run all jobs in parallel ---
    results: list[tuple[str, bool, str, str]] = []  # (label, ok, msg, level)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_level = {
            pool.submit(fslmaths_avg, files, out, dry_run): level
            for files, out, level in jobs
        }
        for future in as_completed(future_to_level):
            level = future_to_level[future]
            lbl, ok, msg = future.result()
            results.append((lbl, ok, msg, level))
            tag = (
                "[dim](dry-run)[/dim]"
                if dry_run
                else ("[green]✓[/green]" if ok else "[red]✗[/red]")
            )
            console.print(
                f"  {tag}  [{level}]  {lbl}" + (f"  [red]{msg}[/red]" if not ok else "")
            )

    # --- Summary ---
    n_ok = sum(1 for _, ok, _, _ in results if ok)
    n_err = len(results) - n_ok
    console.print(
        f"\n[bold]sub-{sub} ses-{ses}:[/bold]  "
        f"[green]{n_ok} completed[/green]"
        + (f"  [red]{n_err} failed[/red]" if n_err else "")
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@app.command()
def main(
    sub: str = typer.Option(None, "-s", help="Subject ID (without sub- prefix)"),
    ses: str = typer.Option(None, "-e", help="Session ID (without ses- prefix)"),
    subseslist: Path = typer.Option(
        None, "-f", help="Path to subseslist CSV (skip header)"
    ),
    bids_dir: Path = typer.Option(..., "-d", help="BIDS root directory"),
    workers: int = typer.Option(4, "-w", help="Number of parallel workers"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print jobs without running FSL"
    ),
) -> None:
    """Average SWI part-mag files at echo / run / all levels using fslmaths in parallel."""

    setup_module("fsl/6.0.3")

    pairs: list[tuple[str, str]] = []

    if subseslist is not None:
        lines = subseslist.read_text().splitlines()
        for line in lines[1:]:
            parts = line.strip().split(",")
            if len(parts) >= 2 and parts[0] and parts[1]:
                pairs.append((parts[0].strip(), parts[1].strip()))
    elif sub and ses:
        pairs = [(sub, ses)]
    else:
        console.print("[red]Provide -s <sub> -e <ses>  or  -f <subseslist>[/red]")
        raise typer.Exit(1)

    for s, e in pairs:
        process_subject(s, e, bids_dir, workers, dry_run)


if __name__ == "__main__":
    app()
