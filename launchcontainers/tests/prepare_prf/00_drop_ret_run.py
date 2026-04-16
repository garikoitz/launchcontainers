#!/usr/bin/env python3
# MIT License
# Copyright (c) 2024-2025 Yongning Lei
"""
Drop a bad ret run from BIDS and vistadisplog, then renumber surviving runs.

What this does for the specified sub/ses/task/run:

  Step 1  BIDS func — remove all files matching:
              sub-{sub}_ses-{ses}_task-{task}_run-{run}_*
          (bold.nii.gz, bold.json, sbref.nii.gz, sbref.json, gfactor.nii.gz, etc.)

  Step 2  vistadisplog — for the symlink:
              sub-{sub}_ses-{ses}_task-{task}_run-{run}_params.mat  →  20XXXXXX.mat
          rename the TARGET file:  20XXXXXX.mat  →  wrongrun_20XXXXXX.mat
          then remove the symlink itself

  Step 3  Renumber — for all surviving runs of the same task with run > dropped run:
              BIDS files:   run-{N}  →  run-{N-1}
              vistadisplog symlinks: delete old link, recreate pointing to same target

Dry-run (default) prints every action without touching any file.
Use --execute to apply.

Usage:
    python drop_ret_run.py -s 10,01 --task retRW --run 01 --bidsdir /path/BIDS
    python drop_ret_run.py -s 10,01 --task retRW --run 01 --bidsdir /path/BIDS --execute
## Note this code is not so automatic, need to check more carefully before and after and need to according to the subseslist xlsx
"""

from __future__ import annotationsnot

import re
from pathlib import Path

import typer
from rich.console import Console

from launchcontainers.utils import reorder_bids_runs

app = typer.Typer(add_completion=False)
console = Console()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def func_files_for_run(
    func_dir: Path, sub: str, ses: str, task: str, run: str
) -> list[Path]:
    """All files in func/ matching sub-X_ses-X_task-X_run-RR_*"""
    prefix = f"sub-{sub}_ses-{ses}_task-{task}_run-{run}_"
    return sorted(func_dir.glob(f"{prefix}*"))


def all_task_runs(func_dir: Path, sub: str, ses: str, task: str) -> list[str]:
    """Sorted list of run labels ('01','02',...) present for a task in BIDS func/."""
    prefix = f"sub-{sub}_ses-{ses}_task-{task}_run-"
    runs: set[str] = set()
    for f in func_dir.glob(f"{prefix}*"):
        m = re.search(r"_run-(\d+)_", f.name)
        if m:
            runs.add(m.group(1))
    return sorted(runs)


def symlink_for_run(
    vdl_dir: Path, sub: str, ses: str, task: str, run: str
) -> Path | None:
    """Return the params.mat symlink path, or None if not found."""
    p = vdl_dir / f"sub-{sub}_ses-{ses}_task-{task}_run-{run}_params.mat"
    return p if p.exists() or p.is_symlink() else None


def all_task_symlinks(vdl_dir: Path, sub: str, ses: str, task: str) -> dict[str, Path]:
    """Return {run_label: symlink_path} for all params.mat symlinks of a task."""
    pattern = f"sub-{sub}_ses-{ses}_task-{task}_run-*_params.mat"
    result: dict[str, Path] = {}
    for p in vdl_dir.glob(pattern):
        m = re.search(r"_run-(\d+)_params\.mat$", p.name)
        if m:
            result[m.group(1)] = p
    return result


# ---------------------------------------------------------------------------
# Action primitives — all log what they do, only execute when dry_run=False
# ---------------------------------------------------------------------------


def action_remove_file(path: Path, dry_run: bool) -> None:
    console.print(f"  [red]DELETE[/red]  {path}")
    if not dry_run:
        path.unlink()


def action_rename(src: Path, dst: Path, dry_run: bool) -> None:
    console.print(f"  [yellow]RENAME[/yellow]  {src.name}  →  {dst.name}")
    if not dry_run:
        src.rename(dst)


def action_remove_symlink(link: Path, dry_run: bool) -> None:
    console.print(f"  [red]UNLINK[/red]  {link.name}")
    if not dry_run:
        link.unlink()


def action_create_symlink(link: Path, target_name: str, dry_run: bool) -> None:
    console.print(f"  [green]SYMLINK[/green]  {link.name}  →  {target_name}")
    if not dry_run:
        link.symlink_to(target_name)


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------


def drop_run(
    sub: str,
    ses: str,
    task: str,
    run: str,  # zero-padded, e.g. "01"
    bids_dir: Path,
    dry_run: bool,
    skip_mat: bool = False,
) -> None:
    func_dir = bids_dir / f"sub-{sub}" / f"ses-{ses}" / "func"
    vdl_dir = bids_dir / "sourcedata" / "vistadisplog" / f"sub-{sub}" / f"ses-{ses}"

    if not func_dir.exists():
        console.print(f"[red]func dir not found:[/red] {func_dir}")
        raise typer.Exit(1)
    if not skip_mat and not vdl_dir.exists():
        console.print(f"[red]vistadisplog dir not found:[/red] {vdl_dir}")
        raise typer.Exit(1)

    mode_tag = (
        "[bold yellow]DRY-RUN[/bold yellow]"
        if dry_run
        else "[bold red]EXECUTE[/bold red]"
    )
    console.rule(f"sub-{sub}  ses-{ses}  task-{task}  run-{run}  {mode_tag}")

    # -------------------------------------------------------------------------
    # Step 1: remove BIDS files for the dropped run
    # -------------------------------------------------------------------------
    console.print("\n[bold]Step 1 — Remove BIDS func files[/bold]")
    bids_files = func_files_for_run(func_dir, sub, ses, task, run)
    if not bids_files:
        console.print(
            f"  [yellow]No BIDS files found for task-{task} run-{run}[/yellow]"
        )
    for f in bids_files:
        action_remove_file(f, dry_run)

    # -------------------------------------------------------------------------
    # Step 2: rename target .mat → wrongrun_*.mat and remove symlink
    # -------------------------------------------------------------------------
    console.print(
        "\n[bold]Step 2 — Retire vistadisplog source file and remove symlink[/bold]"
    )
    if skip_mat:
        console.print("  [dim]--skip-mat: skipping vistadisplog changes.[/dim]")
    elif not vdl_dir.exists():
        console.print(
            "  [yellow]vistadisplog dir not found — skipping mat step.[/yellow]"
        )
    else:
        link = symlink_for_run(vdl_dir, sub, ses, task, run)
        if link is None:
            console.print(
                f"  [yellow]No params.mat symlink found for task-{task} run-{run}[/yellow]"
            )
        else:
            if link.is_symlink():
                target_name = Path(link).readlink().name  # e.g. 20250506T104255.mat
                target_path = vdl_dir / target_name

                # Rename source to wrongrun_*
                wrongrun_name = f"wrongrun_{target_name}"
                wrongrun_path = vdl_dir / wrongrun_name
                if target_path.exists():
                    action_rename(target_path, wrongrun_path, dry_run)
                else:
                    console.print(
                        f"  [yellow]Target {target_name} not found in vdl_dir — skip rename[/yellow]"
                    )

                # Remove the symlink
                action_remove_symlink(link, dry_run)
            else:
                console.print(
                    f"  [yellow]{link.name} exists but is not a symlink — skipping[/yellow]"
                )

    # -------------------------------------------------------------------------
    # Step 3: renumber surviving runs (run > dropped_run get decremented)
    # -------------------------------------------------------------------------
    console.print("\n[bold]Step 3 — Renumber surviving runs[/bold]")
    drop_int = int(run)

    # BIDS renumbering
    all_runs = all_task_runs(func_dir, sub, ses, task)
    surviving = [r for r in all_runs if int(r) > drop_int]

    if not surviving:
        console.print("  No runs to renumber in BIDS.")
    else:
        all_surviving_files: list[Path] = []
        for r in surviving:
            all_surviving_files.extend(func_files_for_run(func_dir, sub, ses, task, r))
        run_map = {int(r): int(r) - 1 for r in surviving}
        reorder_bids_runs(all_surviving_files, run_map, zero_pad=2, dry_run=dry_run)

    # vistadisplog symlink renumbering
    if skip_mat or not vdl_dir.exists():
        surviving_links = {}
    else:
        all_links = all_task_symlinks(vdl_dir, sub, ses, task)
        surviving_links = {r: p for r, p in all_links.items() if int(r) > drop_int}

    if not surviving_links:
        console.print("  No symlinks to renumber in vistadisplog.")
    else:
        # Collect all moves first, then remove all old links, then create new ones.
        # This avoids FileExistsError when run-03→02 tries to create run-02 while it
        # still exists (it is only removed in the next iteration otherwise).
        moves: list[tuple[Path, Path, str]] = []  # (old_link, new_link, target_name)
        for old_run in sorted(surviving_links, reverse=True):
            new_run = f"{int(old_run) - 1:02d}"
            old_link = surviving_links[old_run]
            new_link = vdl_dir / re.sub(r"_run-\d+_", f"_run-{new_run}_", old_link.name)

            if old_link.is_symlink():
                target_name = Path(old_link).readlink().name
                moves.append((old_link, new_link, target_name))
            else:
                console.print(
                    f"  [yellow]{old_link.name} is not a symlink — skip[/yellow]"
                )

        # Pass 1: remove all old symlinks
        for old_link, _, _ in moves:
            action_remove_symlink(old_link, dry_run)

        # Pass 2: create all new symlinks
        for _, new_link, target_name in moves:
            action_create_symlink(new_link, target_name, dry_run)

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    console.print()
    if dry_run:
        console.print(
            "[bold yellow]Dry-run complete — no files were changed. "
            "Use --execute to apply.[/bold yellow]"
        )
    else:
        console.print("[bold green]Done.[/bold green]")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@app.command()
def main(
    subses: str = typer.Option(..., "-s", help="sub,ses (e.g. 10,01)"),
    task: str = typer.Option(
        ..., "--task", help="Task label to drop, without task- prefix (e.g. retRW)"
    ),
    run: str = typer.Option(
        ..., "--run", help="Run label to drop, zero-padded (e.g. 01 or 02)"
    ),
    bids_dir: Path = typer.Option(
        Path("/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS"),
        "-b",
        "--bidsdir",
        help="BIDS root directory",
    ),
    execute: bool = typer.Option(
        False, "--execute", help="Apply changes (default is dry-run)"
    ),
    skip_mat: bool = typer.Option(
        False,
        "--skip-mat",
        help="Skip vistadisplog changes (use when .mat was already renamed manually).",
    ),
) -> None:
    """Drop a bad ret run from BIDS and vistadisplog, then renumber surviving runs."""

    parts = subses.split(",")
    if len(parts) != 2:
        console.print("[red]Error: -s must be sub,ses (e.g. 10,01)[/red]")
        raise typer.Exit(1)

    sub, ses = parts[0].strip(), parts[1].strip()
    run_padded = run.zfill(2)

    drop_run(
        sub=sub,
        ses=ses,
        task=task,
        run=run_padded,
        bids_dir=bids_dir,
        dry_run=not execute,
        skip_mat=skip_mat,
    )


if __name__ == "__main__":
    app()
