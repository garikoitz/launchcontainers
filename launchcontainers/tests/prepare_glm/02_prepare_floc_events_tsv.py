#!/usr/bin/env python3
# -----------------------------------------------------------------------------
# Copyright (c) Yongning Lei 2024-2025
# MIT License
# -----------------------------------------------------------------------------
"""
Link fLoc events.tsv files from sourcedata into the BIDS func directories.

For each fLoc bold run in BIDS, create a symlink:
    BIDS/sub-XX/ses-XX/func/sub-XX_ses-XX_task-fLoc_run-YY_events.tsv
    → sourcedata/sub-XX/ses-*/sub-XX_ses-XX*_1back_*/*_task-fLoc_run-YY_events.tsv

The onset directory is found by scanning all sourcedata ses-* subdirs for a
folder whose name starts with sub-{sub}_ses-{ses} (handles session label
quirks like ses-01rr, ses-01wrong stored under the wrong ses- dir).

Extra runs (run-11, run-12, run-13, ...) are reruns of aborted earlier runs.
Their events.tsv is found via the rerun map (default:
sourcedata/qc/rerun_check.tsv).  The map columns used are:
  sub, ses, task, extra_run, compensates_run
meaning BIDS run extra_run should be linked to the sourcedata events.tsv
of run compensates_run.

Usage
-----
    # Dry-run (default): one-line summary per session
    python prepare_floc_events_tsv.py -s 01,01
    python prepare_floc_events_tsv.py -f subseslist.tsv

    # Verbose: full per-run table
    python prepare_floc_events_tsv.py -s 01,01 -v

    # Apply
    python prepare_floc_events_tsv.py -s 01,01 --execute
    python prepare_floc_events_tsv.py -f subseslist.tsv --execute --force
"""
from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich import box
from rich.text import Text

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
_BIDS_DIR = Path("/scratch/tlei/VOTCLOC/BIDS")
_RERUN_MAP_DEFAULT = Path("sourcedata/qc/rerun_check.tsv")  # relative to bids_dir
_TASK = "fLoc"

# Type alias: (sub, ses, task, extra_run) → compensates_run (zero-padded str)
RerunMap = dict[tuple[str, str, str, str], str]

console = Console()
app = typer.Typer(add_completion=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_onset_dir(sourcedata_dir: Path, sub: str, ses: str) -> tuple[Path | None, str]:
    """
    Return (onset_dir_path, status_message).

    Scans all ses-* subdirs under sourcedata_dir/sub-{sub}/ and looks for
    a directory whose name starts with sub-{sub}_ses-{ses} and contains
    '1back'.  Excludes dirs whose name starts with 'backup'.
    """
    sub_src = sourcedata_dir / f"sub-{sub}"
    if not sub_src.exists():
        return None, f"sourcedata/sub-{sub} does not exist"

    prefix = f"sub-{sub}_ses-{ses}"
    candidates: list[Path] = []

    for ses_dir in sub_src.iterdir():
        if not ses_dir.is_dir():
            continue
        for onset_dir in ses_dir.iterdir():
            name = onset_dir.name
            if (
                name.startswith(prefix)
                and "1back" in name
                and not name.lower().startswith("backup")
            ):
                candidates.append(onset_dir)

    if not candidates:
        return None, f"no onset dir found matching sub-{sub}_ses-{ses}*_1back_*"

    if len(candidates) == 1:
        return candidates[0], "ok"

    # Prefer the one whose name starts with the EXACT session label
    # (e.g. sub-11_ses-03_ not sub-11_ses-03ME_)
    exact_prefix = f"sub-{sub}_ses-{ses}_"
    exact = [c for c in candidates if c.name.startswith(exact_prefix)]
    if len(exact) == 1:
        note = f"multi-onset (chose exact ses label): {[c.name for c in candidates]}"
        return exact[0], note

    # Prefer the one with task-fLoc in the name
    with_task = [c for c in (exact or candidates) if "task-fLoc" in c.name]
    if len(with_task) == 1:
        return with_task[0], f"multi-onset (chose task-fLoc): {[c.name for c in candidates]}"

    # Still ambiguous — pick longest name (most specific) as fallback
    pool = exact or candidates
    pool.sort(key=lambda p: len(p.name), reverse=True)
    return pool[0], f"AMBIGUOUS onset dirs: {[c.name for c in candidates]}"


def _find_source_events(onset_dir: Path, run: str) -> Path | None:
    """Inside onset_dir, find *_task-fLoc_run-{run}_events.tsv (any prefix)."""
    pattern = re.compile(rf".*_task-{_TASK}_run-{run}_events\.tsv$")
    for f in onset_dir.iterdir():
        if pattern.match(f.name):
            return f
    return None


def _load_rerun_map(rerun_tsv: Path) -> RerunMap:
    """
    Load rerun_check.tsv → dict keyed by
    (sub, ses, task, extra_run) → compensates_run (zero-padded to 2 digits).
    """
    mapping: RerunMap = {}
    if not rerun_tsv.exists():
        return mapping
    with rerun_tsv.open() as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            sub  = str(row["sub"]).strip().zfill(2)
            ses  = str(row["ses"]).strip().zfill(2)
            task = str(row["task"]).strip()
            extra = str(int(row["extra_run"])).zfill(2)
            comp  = str(int(row["compensates_run"])).zfill(2)
            mapping[(sub, ses, task, extra)] = comp
    return mapping


def _resolve_original_run(
    rerun_map: RerunMap, sub: str, ses: str, task: str, run: str
) -> tuple[str, list[str]] | tuple[None, list[str]]:
    """
    Follow the compensates chain to find the ultimate original (non-extra) run.

    For a chain  run-11 → run-10 → ... → run-06 → run-04,
    where run-04 is a standard run (not in the map as an extra_run),
    returns ("04", ["10", "09", "08", "07", "06"]).

    Returns (None, []) if run is not in the rerun_map at all.
    The second element is the list of intermediate extra runs traversed
    (empty when only one hop).
    """
    comp = rerun_map.get((sub, ses, task, run))
    if comp is None:
        return None, []

    chain: list[str] = []
    visited: set[str] = {run}

    while (sub, ses, task, comp) in rerun_map:
        if comp in visited:
            break  # cycle guard
        visited.add(comp)
        chain.append(comp)
        comp = rerun_map[(sub, ses, task, comp)]

    return comp, chain


def _atomic_symlink(tgt: Path, src: Path) -> None:
    """
    Replace (or create) *tgt* as a symlink pointing to *src*, atomically.

    Uses a sibling temp file + os.rename, which is an atomic operation on
    POSIX systems.  This avoids the unlink→symlink window where the target
    file would be momentarily absent.
    """
    tmp = tgt.parent / f".tmp_{tgt.name}"
    if tmp.exists() or tmp.is_symlink():
        tmp.unlink()
    tmp.symlink_to(src)
    tmp.rename(tgt)


def _bids_events_path(func_dir: Path, sub: str, ses: str, run: str) -> Path:
    return func_dir / f"sub-{sub}_ses-{ses}_task-{_TASK}_run-{run}_events.tsv"


def _get_floc_runs(func_dir: Path, sub: str, ses: str) -> list[str]:
    """Return sorted run labels (e.g. ['01', '02', ...]) from bold files."""
    pattern = re.compile(
        rf"sub-{sub}_ses-{ses}_task-{_TASK}_run-(\d+)_bold\.nii\.gz"
    )
    runs: list[str] = []
    for f in func_dir.glob(f"*task-{_TASK}*bold.nii.gz"):
        m = pattern.match(f.name)
        if m:
            runs.append(m.group(1))
    return sorted(runs)


# ---------------------------------------------------------------------------
# Status colours
# ---------------------------------------------------------------------------
_STATUS_STYLE: dict[str, str] = {
    "LINK":       "bold green",
    "SKIP":       "dim green",
    "WRONG":      "bold yellow",
    "FILE_EXISTS":"bold yellow",
    "NO_SOURCE":  "bold red",
    "→ FIXED":    "bold cyan",
}

def _styled_status(status: str) -> Text:
    return Text(status, style=_STATUS_STYLE.get(status, "white"))


def _process_session(
    bids_dir: Path,
    sub: str,
    ses: str,
    execute: bool,
    force: bool,
    rerun_map: RerunMap,
    verbose: bool,
) -> tuple[int, int, int, int, bool]:
    """
    Process one sub/ses.

    Returns (n_link, n_skip, n_wrong, n_missing, had_error).
    """
    func_dir = bids_dir / f"sub-{sub}" / f"ses-{ses}" / "func"
    sourcedata_dir = bids_dir / "sourcedata"

    if not func_dir.exists():
        if verbose:
            console.print(f"  [dim]SKIP[/dim]  func dir not found: {func_dir}")
        return 0, 0, 0, 0, False

    runs = _get_floc_runs(func_dir, sub, ses)
    if not runs:
        if verbose:
            console.print(f"  [dim]SKIP[/dim]  no {_TASK} bold files")
        return 0, 0, 0, 0, False

    onset_dir, onset_status = _find_onset_dir(sourcedata_dir, sub, ses)
    had_error = onset_dir is None
    onset_warnings: list[str] = []

    if onset_dir is None:
        if verbose:
            console.print(f"  [bold red]ERROR[/bold red]  {onset_status}")
        return 0, 0, 0, len(runs), True

    if onset_status != "ok":
        onset_warnings.append(onset_status)

    # ---- per-run pass ----
    n_ok = n_skip = n_missing = n_warn = 0
    rows: list[tuple[str, str, str]] = []   # (run, status, detail) for verbose table

    for run in runs:
        tgt = _bids_events_path(func_dir, sub, ses, run)
        rerun_note = ""

        # Check rerun_map FIRST: if this run is a rerun (in the map), use the
        # direct compensates_run (one hop) rather than the direct sourcedata file.
        # Without this, within-range reruns (e.g. run-06…run-10) would find their
        # own aborted events.tsv in sourcedata and get a self-symlink, making them
        # invisible to generate_rerun_check_from_bids.py.
        #
        # We use ONE hop only so that the symlink target filename encodes the
        # direct compensates relationship (matching the authoritative rerun_check).
        # Full chain resolution is only a fallback when the direct run's events.tsv
        # doesn't exist in sourcedata.
        direct_comp = rerun_map.get((sub, ses, _TASK, run))
        if direct_comp is not None:
            src = _find_source_events(onset_dir, direct_comp)
            if src is not None:
                rerun_note = f" [dim](rerun→run-{direct_comp})[/dim]"
            else:
                # Direct comp not in sourcedata; fall back to full chain resolution
                comp_run, chain = _resolve_original_run(rerun_map, sub, ses, _TASK, run)
                if comp_run is not None:
                    src = _find_source_events(onset_dir, comp_run)
                    chain_str = f"via {' → '.join('run-' + r for r in chain)} → " if chain else ""
                    rerun_note = (
                        f" [dim](rerun→{chain_str}run-{comp_run})[/dim]"
                        if src else f" [yellow](rerun map: {chain_str}run-{comp_run} not in onset dir)[/yellow]"
                    )
                if src is None:
                    rerun_note = f" [yellow](rerun map: run-{direct_comp} not in onset dir)[/yellow]"
        else:
            src = _find_source_events(onset_dir, run)

        if src is None:
            status = "NO_SOURCE"
            detail = f"not found{rerun_note or ' (not in rerun map)'}"
            n_missing += 1
        elif tgt.is_symlink():
            if tgt.resolve() == src.resolve():
                status = "SKIP"
                detail = f"already correct{rerun_note}"
                n_skip += 1
            else:
                status = "WRONG"
                detail = f"points to [italic]{tgt.readlink().name}[/italic]"
                n_warn += 1
        elif tgt.exists():
            status = "FILE_EXISTS"
            detail = "real file (not symlink) already present"
            n_warn += 1
        else:
            status = "LINK"
            detail = f"[dim]{src.name}[/dim]{rerun_note}"
            n_ok += 1

        rows.append((run, status, detail))

        # Apply
        if execute:
            if status == "LINK":
                tgt.symlink_to(src)
            elif status in ("SKIP", "WRONG") and force:
                _atomic_symlink(tgt, src)
                rows[-1] = (run, "→ FIXED", detail)
            elif status == "WRONG" and not force:
                pass  # will be visible in the table

    # ---- output ----
    if verbose:
        # Onset dir info
        rel = onset_dir.relative_to(sourcedata_dir)
        console.print(f"  [cyan]onset[/cyan] : {rel}")
        for w in onset_warnings:
            console.print(f"  [yellow]WARN[/yellow]  {w}")

        # Per-run table
        tbl = Table(box=box.SIMPLE, show_header=True, padding=(0, 1))
        tbl.add_column("run",    style="bold", no_wrap=True)
        tbl.add_column("status", no_wrap=True)
        tbl.add_column("source")
        for run, status, detail in rows:
            tbl.add_row(run, _styled_status(status), Text.from_markup(detail))
        console.print(tbl)

    return n_ok, n_skip, n_warn, n_missing, n_missing > 0 or n_warn > 0


def _iter_subses(
    bids_dir: Path,
    subses_arg: Optional[str],
    file_arg: Optional[Path],
) -> list[tuple[str, str]]:
    """Return list of (sub, ses) pairs to process."""
    if subses_arg:
        parts = subses_arg.split(",")
        if len(parts) != 2:
            console.print("[red]Error:[/red] -s expects sub,ses  (e.g. -s 01,01)")
            raise typer.Exit(1)
        return [(parts[0].strip(), parts[1].strip())]

    if file_arg:
        pairs: list[tuple[str, str]] = []
        with file_arg.open() as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.lower().startswith("sub"):
                    continue
                parts = re.split(r"[,\t]+", line)
                if len(parts) >= 2:
                    pairs.append((parts[0].strip(), parts[1].strip()))
        return pairs

    # Auto-discover all BIDS sessions with fLoc data
    pairs = []
    for sub_dir in sorted(bids_dir.glob("sub-*")):
        if not sub_dir.is_dir():
            continue
        sub = sub_dir.name.replace("sub-", "")
        for ses_dir in sorted(sub_dir.glob("ses-*")):
            if not ses_dir.is_dir():
                continue
            ses = ses_dir.name.replace("ses-", "")
            func_dir = ses_dir / "func"
            if func_dir.exists() and list(func_dir.glob(f"*task-{_TASK}*bold.nii.gz")):
                pairs.append((sub, ses))
    return pairs


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@app.command()
def main(
    bids_dir: Path = typer.Option(
        _BIDS_DIR,
        "--bids-dir", "-b",
        help="BIDS root directory",
    ),
    subses_arg: Optional[str] = typer.Option(
        None, "-s",
        help="Single sub,ses pair  e.g.  -s 01,01",
    ),
    file_arg: Optional[Path] = typer.Option(
        None, "-f",
        help="TSV/CSV with sub,ses columns (header optional)",
    ),
    execute: bool = typer.Option(
        False, "--execute",
        help="Apply changes. Default is dry-run.",
    ),
    force: bool = typer.Option(
        False, "--force",
        help="Overwrite WRONG symlinks.",
    ),
    rerun_tsv: Optional[Path] = typer.Option(
        None, "--rerun-map",
        help="Path to rerun_check.tsv (default: <bids_dir>/sourcedata/qc/rerun_check.tsv)",
    ),
    verbose: bool = typer.Option(
        False, "-v", "--verbose",
        help="Show per-run table for every session (default: one summary line per session).",
    ),
) -> None:
    """
    Link fLoc events.tsv from sourcedata into BIDS func directories.

    Default is dry-run. Pass --execute to create symlinks.
    Pass --force to also fix symlinks that point to the wrong target.
    Pass -v for a full per-run breakdown.
    """
    if not bids_dir.exists():
        console.print(f"[red]Error:[/red] BIDS dir not found: {bids_dir}")
        raise typer.Exit(1)

    if subses_arg and file_arg:
        console.print("[red]Error:[/red] -s and -f are mutually exclusive")
        raise typer.Exit(1)

    # Load rerun map
    rerun_path = rerun_tsv if rerun_tsv else bids_dir / _RERUN_MAP_DEFAULT
    rerun_map = _load_rerun_map(rerun_path)

    pairs = _iter_subses(bids_dir, subses_arg, file_arg)
    if not pairs:
        console.print("[yellow]No sub/ses pairs found.[/yellow]")
        raise typer.Exit(0)

    mode_str = "[bold green]EXECUTE[/bold green]" if execute else "[bold yellow]DRY-RUN[/bold yellow]"
    rerun_str = (
        f"[green]{rerun_path.name}[/green] ({len(rerun_map)} entries)"
        if rerun_map
        else f"[yellow]not found[/yellow] ({rerun_path})"
    )
    console.print(f"\n[bold]BIDS dir[/bold]  : {bids_dir}")
    console.print(f"[bold]Mode[/bold]      : {mode_str}")
    console.print(f"[bold]Sessions[/bold]  : {len(pairs)}")
    console.print(f"[bold]Rerun map[/bold] : {rerun_str}")
    console.print()

    total_link = total_skip = total_wrong = total_missing = 0
    n_problem = 0

    for sub, ses in pairs:
        label = f"sub-{sub} ses-{ses}"

        if verbose:
            console.rule(f"[bold]{label}[/bold]", style="bright_black")

        n_ok, n_skip, n_warn, n_missing, has_problem = _process_session(
            bids_dir, sub, ses,
            execute=execute, force=force,
            rerun_map=rerun_map, verbose=verbose,
        )

        total_link    += n_ok
        total_skip    += n_skip
        total_wrong   += n_warn
        total_missing += n_missing
        if has_problem:
            n_problem += 1

        # One-line summary (always printed; in verbose mode it comes after the table)
        if n_missing > 0 or n_warn > 0:
            icon = ":x:"
            summary_style = "bold red"
        else:
            icon = ":white_check_mark:"
            summary_style = "green"

        parts = []
        if n_ok:    parts.append(f"link={n_ok}")
        if n_skip:  parts.append(f"skip={n_skip}")
        if n_warn:  parts.append(f"[yellow]wrong={n_warn}[/yellow]")
        if n_missing: parts.append(f"[red]no_source={n_missing}[/red]")
        counts = "  ".join(parts) if parts else "—"

        console.print(
            f"  {icon}  [{summary_style}]{label}[/{summary_style}]"
            f"  {counts}"
        )

    # ---- final summary ----
    console.print()
    console.rule(style="bright_black")
    ok_col   = "green" if total_missing == 0 and total_wrong == 0 else "yellow"
    console.print(
        f"[bold]Total[/bold]  "
        f"link=[{ok_col}]{total_link}[/{ok_col}]  "
        f"skip=[dim]{total_skip}[/dim]  "
        f"wrong=[{'bold red' if total_wrong else 'dim'}]{total_wrong}[/{'bold red' if total_wrong else 'dim'}]  "
        f"no_source=[{'bold red' if total_missing else 'dim'}]{total_missing}[/{'bold red' if total_missing else 'dim'}]  "
        f"sessions_with_issues=[{'bold red' if n_problem else 'dim'}]{n_problem}[/{'bold red' if n_problem else 'dim'}]"
    )
    mode_label = "EXECUTED" if execute else "DRY-RUN"
    console.print(f"[dim]{mode_label}[/dim]", end="")
    if not execute:
        console.print("  →  re-run with [bold]--execute[/bold] to apply", end="")
    console.print()


if __name__ == "__main__":
    app()
