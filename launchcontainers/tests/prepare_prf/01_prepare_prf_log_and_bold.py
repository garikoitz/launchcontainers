#!/usr/bin/env python3
import typer
from pathlib import Path
from datetime import datetime, timedelta
import scipy.io as sio
import json
from rich.console import Console
from rich.table import Table

from launchcontainers.utils import atomic_rename_pairs, parse_subses_list

app = typer.Typer()
console = Console()

# ---------------------------------------------------------------------------
# Verbosity gate — set once by check command, read everywhere
# ---------------------------------------------------------------------------

_verbose: bool = False


def _vprint(msg: str) -> None:
    """Print only when -v is passed."""
    if _verbose:
        console.print(msg)



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
    """Match BIDS files (bold, sbref) to .mat files by positional order (both sorted by time)."""

    all_ret_files = list(bids_dir.glob(f"sub-{sub}/ses-{ses}/func/*task-ret*.nii.gz"))
    bold_files = [f for f in all_ret_files if "_bold.nii.gz" in f.name]
    sbref_files = [f for f in all_ret_files if "_sbref.nii.gz" in f.name]

    _vprint(f"[yellow]Found: {len(bold_files)} bold, {len(sbref_files)} sbref")

    dummy_date = datetime(2000, 1, 1)

    def file_info(nii_file, file_type):
        json_file = nii_file.with_suffix("").with_suffix(".json")
        file_dt = get_bids_datetime(json_file)
        parts = nii_file.stem.replace(".nii", "").split("_")
        file_task = file_run = None
        for part in parts:
            if part.startswith("task-"):
                file_task = part.replace("task-", "")
            elif part.startswith("run-"):
                file_run = int(part.replace("run-", ""))
        return {
            "nii_file": nii_file,
            "json_file": json_file,
            "file_dt": file_dt,
            "file_task": file_task,
            "file_run": file_run,
            "file_type": file_type,
        }

    # Build info lists sorted by acquisition time
    bold_infos = sorted(
        [file_info(f, "bold") for f in bold_files],
        key=lambda x: datetime.combine(dummy_date, x["file_dt"].time()) if x["file_dt"] else datetime.min,
    )
    sbref_infos = sorted(
        [file_info(f, "sbref") for f in sbref_files],
        key=lambda x: datetime.combine(dummy_date, x["file_dt"].time()) if x["file_dt"] else datetime.min,
    )

    # Sort mat_map by datetime
    sorted_mats = sorted(
        mat_map,
        key=lambda m: datetime.combine(dummy_date, m["datetime"].time()) if m["datetime"] else datetime.min,
    )

    mat_labels = [m["task"] + "-" + str(m["run"]) for m in sorted_mats]
    bold_labels = [str(b["file_task"]) + "-" + str(b["file_run"]) for b in bold_infos]
    _vprint(f"[cyan]Mats ({len(sorted_mats)}): {mat_labels}")
    _vprint(f"[cyan]Bolds ({len(bold_infos)}): {bold_labels}")

    def make_entry(nii_info, mat_item):
        return {
            "nii_file": nii_info["nii_file"],
            "json_file": nii_info["json_file"],
            "file_task": nii_info["file_task"],
            "file_run": nii_info["file_run"],
            "mat_task": mat_item["task"],
            "mat_run": mat_item["run"],
            "file_type": nii_info["file_type"],
            "needs_rename": not (
                nii_info["file_task"] == mat_item["task"]
                and nii_info["file_run"] == mat_item["run"]
            ),
        }

    # Match positionally: mat[i] → bold[i], mat[i] → sbref[i]
    n = min(len(sorted_mats), len(bold_infos))
    matches = []

    for i in range(n):
        mat_item = sorted_mats[i]
        bold = make_entry(bold_infos[i], mat_item)
        sbref = make_entry(sbref_infos[i], mat_item) if i < len(sbref_infos) else None
        _vprint(
            f"  [{i}] mat={mat_item['task']}-{mat_item['run']}  "
            f"bold={bold_infos[i]['file_task']}-{bold_infos[i]['file_run']}"
        )
        matches.append({"mat_task": mat_item["task"], "mat_run": mat_item["run"], "bold": bold, "sbref": sbref})

    # Mats without a bold (missing BIDS files)
    for i in range(n, len(sorted_mats)):
        matches.append({"mat_task": sorted_mats[i]["task"], "mat_run": sorted_mats[i]["run"], "bold": None, "sbref": None})

    return matches


@app.command()
def setup(
    bids_dir: Path = typer.Option(..., "--bids", "-b"),
    sub: str = typer.Option(None, "--sub", "-s"),
    ses: str = typer.Option(None, "--ses"),
    file: Path = typer.Option(None, "--file", help="subseslist CSV/TSV with sub,ses columns"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing symlinks"),
    dry_run: bool = typer.Option(True, "--dry-run/--execute"),
):
    """Create vistadisplog/sub-xx/ses-xx symlinks pointing to sourcedata/sub-xx/ses-xx."""
    if file is None and (sub is None or ses is None):
        raise typer.BadParameter("Provide either --file or both --sub and --ses")

    pairs = parse_subses_list(file) if file else [(sub, ses)]
    vistadisplog = bids_dir / "sourcedata" / "vistadisplog"

    for sub, ses in pairs:
        target = bids_dir / "sourcedata" / f"sub-{sub}" / f"ses-{ses}"
        link_dir = vistadisplog / f"sub-{sub}"
        link_path = link_dir / f"ses-{ses}"
        console.print(f"[cyan]sub-{sub} ses-{ses}[/cyan]")
        console.print(f"  link : {link_path}")
        console.print(f"  → target : {target}")

        if not target.exists():
            console.print(f"  [red]✗ target does not exist, skipping[/red]")
            continue

        if link_path.exists() or link_path.is_symlink():
            if not force:
                console.print(f"  [yellow]already exists, skipping (use --force to overwrite)[/yellow]")
                continue
            if not dry_run:
                link_path.unlink()
                console.print(f"  [yellow]removed existing symlink[/yellow]")

        if not dry_run:
            link_dir.mkdir(parents=True, exist_ok=True)
            link_path.symlink_to(target)
            console.print(f"  [green]✓ created[/green]")
        else:
            console.print(f"  [yellow](dry run)[/yellow]")

    if dry_run:
        console.print("\n[yellow]DRY RUN — use --execute to create symlinks.[/yellow]")


@app.command()
def link(
    bids_dir: Path = typer.Option(..., "--bids", "-b"),
    sub: str = typer.Option(None, "--sub", "-s"),
    ses: str = typer.Option(None, "--ses"),
    file: Path = typer.Option(None, "--file", help="subseslist CSV/TSV with sub,ses columns"),
    force: bool = typer.Option(False, "--force", "-f"),
    dry_run: bool = typer.Option(True, "--dry-run/--execute"),
):
    """Create symlinks for .mat files based on stimName."""
    if file is None and (sub is None or ses is None):
        raise typer.BadParameter("Provide either --file or both --sub and --ses")

    pairs = parse_subses_list(file) if file else [(sub, ses)]
    vistadisplog = bids_dir / "sourcedata" / "vistadisplog"

    for sub, ses in pairs:
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
    sub: str = typer.Option(None, "--sub", "-s"),
    ses: str = typer.Option(None, "--ses"),
    file: Path = typer.Option(None, "--file", help="subseslist CSV/TSV with sub,ses columns"),
    max_gap: int = typer.Option(200, "--max-gap"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Print per-run match details"),
):
    """Check if BIDS files match .mat files."""
    global _verbose
    _verbose = verbose

    if file is None and (sub is None or ses is None):
        raise typer.BadParameter("Provide either --file or both --sub and --ses")

    pairs = parse_subses_list(file) if file else [(sub, ses)]
    vistadisplog = bids_dir / "sourcedata" / "vistadisplog"

    for sub, ses in pairs:
        symlink_map = create_mat_symlinks(vistadisplog, sub, ses, dry_run=True)
        matches = match_bids_files(symlink_map, bids_dir, sub, ses, max_gap)

        n_ok = sum(1 for m in matches if m["bold"] and not m["bold"]["needs_rename"])
        n_mismatch = sum(1 for m in matches if m["bold"] and m["bold"]["needs_rename"])
        n_missing = sum(1 for m in matches if not m["bold"])

        status = "[green]OK[/green]" if n_mismatch == 0 and n_missing == 0 else "[red]ISSUES[/red]"
        console.print(
            f"sub-{sub} ses-{ses}  {status}  "
            f"matched={n_ok}  mismatch={n_mismatch}  missing={n_missing}"
        )

        if verbose:
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("MAT Task")
            table.add_column("MAT Run")
            table.add_column("Bold")
            table.add_column("SBRef")

            for m in matches:
                if m["bold"]:
                    bold_str = (
                        f"[green]✓[/green] {m['bold']['file_task']}-{m['bold']['file_run']:02d}"
                        if not m["bold"]["needs_rename"]
                        else f"[red]✗[/red] {m['bold']['file_task']}-{m['bold']['file_run']:02d}"
                    )
                else:
                    bold_str = "[red]Missing[/red]"

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
    sub: str = typer.Option(None, "--sub", "-s"),
    ses: str = typer.Option(None, "--ses"),
    file: Path = typer.Option(None, "--file", help="subseslist CSV/TSV with sub,ses columns"),
    max_gap: int = typer.Option(200, "--max-gap"),
    dry_run: bool = typer.Option(True, "--dry-run/--execute"),
):
    """Rename BIDS files to match .mat task names."""
    if file is None and (sub is None or ses is None):
        raise typer.BadParameter("Provide either --file or both --sub and --ses")

    pairs = parse_subses_list(file) if file else [(sub, ses)]
    vistadisplog = bids_dir / "sourcedata" / "vistadisplog"

    for sub, ses in pairs:
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
