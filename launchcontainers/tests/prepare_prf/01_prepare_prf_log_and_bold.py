#!/usr/bin/env python3
import typer
from pathlib import Path
from datetime import datetime, timedelta
import scipy.io as sio
import json
from rich.console import Console
from rich.table import Table

from launchcontainers.utils import atomic_rename_pairs

app = typer.Typer()
console = Console()


def parse_mat_datetime(mat_filename: str):
    """Extract datetime from .mat filename: 20250804T111657.mat"""
    try:
        dt_str = mat_filename.replace(".mat", "")
        dt = datetime.strptime(dt_str, "%Y%m%dT%H%M%S")
        adjust_dt = dt - timedelta(minutes=6)
        return adjust_dt
    except Exception:
        return None


def get_stim_name(mat_file: Path):
    """Extract stimName from .mat file."""
    try:
        mat = sio.loadmat(str(mat_file), simplify_cells=True)
        return mat["params"]["loadMatrix"]
    except Exception:
        return None


def create_mat_symlinks(
    vistadisplog: Path, sub: str, ses: str, dry_run: bool = True, force: bool = False
):
    """Create symlinks for .mat files based on stimName."""

    counters = {
        "CB": 1,
        "FF": 1,
        "RW": 1,
        "fixRW": 1,
        "fixFF": 1,
        "fixRWblock": 1,
        "fixRWblock01": 1,
        "fixRWblock02": 1,
    }

    mat_dir = vistadisplog / f"sub-{sub}" / f"ses-{ses}"
    mat_files = sorted(mat_dir.glob("20*.mat"), key=lambda x: x.name)

    symlink_map = []

    for mat_file in mat_files:
        stim_name = get_stim_name(mat_file)
        if not stim_name:
            console.print(f"[red]Cannot read: {mat_file.name}[/red]")
            continue

        mat_dt = parse_mat_datetime(mat_file.name)

        task_name = None
        if "CB_" in stim_name and "tr-2" in stim_name:
            task_name = "retCB"
            run = counters["CB"]
            counters["CB"] += 1
        elif "fixRWblock01_" in stim_name and "tr-2" in stim_name:
            task_name = "retfixRWblock01"
            run = counters["fixRWblock01"]
            counters["fixRWblock01"] += 1
        elif "fixRWblock02_" in stim_name and "tr-2" in stim_name:
            task_name = "retfixRWblock02"
            run = counters["fixRWblock02"]
            counters["fixRWblock02"] += 1
        elif "fixRWblock_" in stim_name and "tr-2" in stim_name:
            task_name = "retfixRWblock"
            run = counters["fixRWblock"]
            counters["fixRWblock"] += 1
        elif "fixFF_" in stim_name and "tr-2" in stim_name:
            task_name = "retfixFF"
            run = counters["fixFF"]
            counters["fixFF"] += 1
        elif "fixRW_" in stim_name and "tr-2" in stim_name:
            task_name = "retfixRW"
            run = counters["fixRW"]
            counters["fixRW"] += 1
        elif "FF_" in stim_name and "tr-2" in stim_name:
            task_name = "retFF"
            run = counters["FF"]
            counters["FF"] += 1
        elif "RW_" in stim_name and "tr-2" in stim_name:
            task_name = "retRW"
            run = counters["RW"]
            counters["RW"] += 1

        if task_name:
            link_name = (
                mat_dir
                / f"sub-{sub}_ses-{ses}_task-{task_name}_run-{run:02d}_params.mat"
            )
            symlink_map.append(
                {
                    "original": mat_file,
                    "link": link_name,
                    "datetime": mat_dt,
                    "task": task_name,
                    "run": run,
                    "stim_name": stim_name,
                }
            )

    if not dry_run:
        for item in symlink_map:
            if item["link"].exists() or item["link"].is_symlink():
                if force:
                    item["link"].unlink()
                    item["link"].symlink_to(item["original"].name)
                    console.print(f"[green]Overwritten: {item['link'].name}[/green]")
                else:
                    console.print(
                        f"[yellow]Skipped (exists): {item['link'].name}[/yellow]"
                    )
            else:
                item["link"].symlink_to(item["original"].name)
                console.print(f"[green]Created: {item['link'].name}[/green]")

    return symlink_map


def get_bids_datetime(json_file: Path):
    """Extract AcquisitionDateTime from BIDS JSON."""
    try:
        with open(json_file) as f:
            data = json.load(f)
        acq_time = data.get("AcquisitionTime")
        if acq_time:
            time_obj = datetime.strptime(acq_time.split(".")[0], "%H:%M:%S")
            return time_obj
    except Exception:
        pass
    return None


def match_bids_files(mat_map, bids_dir: Path, sub: str, ses: str, max_gap: int = 200):
    """Match BIDS files (bold, sbref, gfactor) to .mat files by datetime."""

    # Get ALL ret files (bold, sbref, gfactor)
    all_ret_files = list(bids_dir.glob(f"sub-{sub}/ses-{ses}/func/*task-ret*.nii.gz"))

    # Separate by type
    bold_files = [f for f in all_ret_files if "_bold.nii.gz" in f.name]
    sbref_files = [f for f in all_ret_files if "_sbref.nii.gz" in f.name]

    console.print(f"[yellow]Found: {len(bold_files)} bold, {len(sbref_files)} sbref")

    # Debug the sbref
    console.print("[blue]SBRef files:[/blue]")
    for f in sbref_files:
        console.print(f"{f.name}")
    dummy_date = datetime(2000, 1, 1)

    def match_file_to_mat(nii_file, file_type):
        json_file = nii_file.with_suffix("").with_suffix(".json")
        file_dt = get_bids_datetime(json_file)

        if not file_dt:
            console.print(
                f" [{file_type}] {nii_file.name}: NO ACQUISITION TIME in JSON"
            )
            return None
        # Parse filename
        parts = nii_file.stem.replace(".nii", "").split("_")
        file_task = file_run = None
        for part in parts:
            if part.startswith("task-"):
                file_task = part.replace("task-", "")
            elif part.startswith("run-"):
                file_run = int(part.replace("run-", ""))

        # Find closest .mat match by time
        best_match = None
        min_diff = timedelta(days=999)

        for mat_item in mat_map:
            mat_dt = mat_item["datetime"]
            mat_time = datetime.combine(dummy_date, mat_dt.time())
            file_time = datetime.combine(dummy_date, file_dt.time())
            diff = abs((mat_time - file_time).total_seconds())

            if diff < min_diff.total_seconds() and diff <= max_gap:
                min_diff = timedelta(seconds=diff)
                best_match = mat_item

        if best_match:
            console.print(
                f"  [{file_type}] {nii_file.name}: {file_task}-{file_run} ({file_dt.strftime('%H:%M:%S')}) → {best_match['task']}-{best_match['run']} (diff: {int(min_diff.total_seconds())}s)"
            )
            return {
                "nii_file": nii_file,
                "json_file": json_file,
                "file_task": file_task,
                "file_run": file_run,
                "mat_task": best_match["task"],
                "mat_run": best_match["run"],
                "time_diff": int(min_diff.total_seconds()),
                "file_type": file_type,
                "needs_rename": not (
                    file_task == best_match["task"] and file_run == best_match["run"]
                ),
            }

        return None

    # Match all three types
    bold_matches = [match_file_to_mat(f, "bold") for f in bold_files]
    sbref_matches = [match_file_to_mat(f, "sbref") for f in sbref_files]

    # Remove None values
    bold_matches = [m for m in bold_matches if m]
    sbref_matches = [m for m in sbref_matches if m]

    # Group by mat_task + mat_run
    from collections import defaultdict

    grouped = defaultdict(lambda: {"bold": None, "sbref": None})

    for match in bold_matches:
        key = (match["mat_task"], match["mat_run"])
        grouped[key]["bold"] = match

    for match in sbref_matches:
        key = (match["mat_task"], match["mat_run"])
        grouped[key]["sbref"] = match

    # Convert to list of matches
    matches = []
    for (mat_task, mat_run), files in grouped.items():
        matches.append(
            {
                "mat_task": mat_task,
                "mat_run": mat_run,
                "bold": files["bold"],
                "sbref": files["sbref"],
            }
        )

    return matches


@app.command()
def link(
    bids_dir: Path = typer.Option(..., "--bids", "-b"),
    sub: str = typer.Option(..., "--sub", "-s"),
    ses: str = typer.Option(..., "--ses"),
    force: bool = typer.Option(False, "--force", "-f"),
    dry_run: bool = typer.Option(True, "--dry-run/--execute"),
):
    """Create symlinks for .mat files based on stimName."""
    vistadisplog = bids_dir / "sourcedata" / "vistadisplog"

    console.print(f"[cyan]Creating .mat symlinks for sub-{sub} ses-{ses}[/cyan]")
    console.print(f"[yellow]Dry run: {dry_run}, Force: {force}[/yellow]\n")

    symlink_map = create_mat_symlinks(vistadisplog, sub, ses, dry_run, force)

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Original File")
    table.add_column("Task")
    table.add_column("Run")
    table.add_column("DateTime")

    for item in symlink_map:
        table.add_row(
            item["original"].name,
            item["task"],
            str(item["run"]),
            item["datetime"].strftime("%Y-%m-%d %H:%M:%S"),
        )

    console.print(table)

    if dry_run:
        console.print(
            "\n[yellow]This is a DRY RUN. Use --execute to create symlinks.[/yellow]"
        )


@app.command()
def check(
    bids_dir: Path = typer.Option(..., "--bids", "-b"),
    sub: str = typer.Option(..., "--sub", "-s"),
    ses: str = typer.Option(..., "--ses"),
    max_gap: int = typer.Option(200, "--max-gap"),
):
    """Check if BIDS files match .mat files."""
    vistadisplog = bids_dir / "sourcedata" / "vistadisplog"

    console.print(f"[cyan]Checking matches for sub-{sub} ses-{ses}[/cyan]\n")

    symlink_map = create_mat_symlinks(vistadisplog, sub, ses, dry_run=True)
    matches = match_bids_files(symlink_map, bids_dir, sub, ses, max_gap)

    # Show matches
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("MAT Task")
    table.add_column("MAT Run")
    table.add_column("Bold")
    table.add_column("SBRef")

    for m in matches:
        # Bold status
        if m["bold"]:
            bold_str = (
                f"[green]✓[/green] {m['bold']['file_task']}-{m['bold']['file_run']:02d}"
                if not m["bold"]["needs_rename"]
                else f"[red]✗[/red] {m['bold']['file_task']}-{m['bold']['file_run']:02d}"
            )
        else:
            bold_str = "[red]Missing[/red]"

        # SBRef status
        if m["sbref"]:
            sbref_str = (
                f"[green]✓[/green] {m['sbref']['file_task']}-{m['sbref']['file_run']:02d}"
                if not m["sbref"]["needs_rename"]
                else f"[red]✗[/red] {m['sbref']['file_task']}-{m['sbref']['file_run']:02d}"
            )
        else:
            sbref_str = "[yellow]Missing[/yellow]"

        table.add_row(m["mat_task"], str(m["mat_run"]), bold_str, sbref_str)

    console.print(table)


@app.command()
def rename(
    bids_dir: Path = typer.Option(..., "--bids", "-b"),
    sub: str = typer.Option(..., "--sub", "-s"),
    ses: str = typer.Option(..., "--ses"),
    max_gap: int = typer.Option(200, "--max-gap"),
    dry_run: bool = typer.Option(True, "--dry-run/--execute"),
):
    """Rename BIDS files to match .mat task names."""
    vistadisplog = bids_dir / "sourcedata" / "vistadisplog"

    console.print(f"[cyan]Renaming files for sub-{sub} ses-{ses}[/cyan]")
    console.print(f"[yellow]Dry run: {dry_run}[/yellow]\n")

    symlink_map = create_mat_symlinks(vistadisplog, sub, ses, dry_run=True)
    matches = match_bids_files(symlink_map, bids_dir, sub, ses, max_gap)

    rename_count = 0

    for m in matches:
        console.print(
            f"[cyan]Processing MAT: {m['mat_task']} run-{m['mat_run']:02d}[/cyan]"
        )

        # Collect all rename pairs for this match group (bold + sbref)
        rename_pairs: list[tuple[Path, Path]] = []

        if m["bold"] and m["bold"]["needs_rename"]:
            old_nii = m["bold"]["nii_file"]
            old_json = m["bold"]["json_file"]
            old_base = old_nii.stem.replace(".nii", "")
            new_base = old_base.replace(
                f"task-{m['bold']['file_task']}_run-{m['bold']['file_run']:02d}",
                f"task-{m['mat_task']}_run-{m['mat_run']:02d}",
            )
            new_nii = old_nii.parent / f"{new_base}.nii.gz"
            new_json = old_json.parent / f"{new_base}.json"
            console.print(f"  [yellow]Bold:[/yellow] {old_nii.name} → {new_nii.name}")
            rename_pairs.append((old_nii, new_nii))
            if old_json.exists():
                rename_pairs.append((old_json, new_json))

        if m["sbref"] and m["sbref"]["needs_rename"]:
            old_nii = m["sbref"]["nii_file"]
            old_json = m["sbref"]["json_file"]
            old_base = old_nii.stem.replace(".nii", "")
            new_base = old_base.replace(
                f"task-{m['sbref']['file_task']}_run-{m['sbref']['file_run']:02d}",
                f"task-{m['mat_task']}_run-{m['mat_run']:02d}",
            )
            new_nii = old_nii.parent / f"{new_base}.nii.gz"
            new_json = old_json.parent / f"{new_base}.json"
            console.print(f"  [yellow]SBRef:[/yellow] {old_nii.name} → {new_nii.name}")
            rename_pairs.append((old_nii, new_nii))
            if old_json.exists():
                rename_pairs.append((old_json, new_json))

        if rename_pairs:
            try:
                atomic_rename_pairs(rename_pairs, dry_run=dry_run)
            except RuntimeError as exc:
                console.print(f"  [red]ERROR[/red] {exc}")

        if m["bold"] or m["sbref"]:
            rename_count += 1
            console.print()

    if dry_run:
        console.print(
            f"\n[yellow]DRY RUN - would rename {rename_count} groups. Use --execute.[/yellow]"
        )
    else:
        console.print(f"\n[green]Renamed {rename_count} groups (bold+sbref)[/green]")


if __name__ == "__main__":
    app()
