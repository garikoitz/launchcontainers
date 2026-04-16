"""
MIT License
Copyright (c) 2024-2025 Yongning Lei

Special fix — sub-01 ses-09 and ses-10
=======================================
Context (from analysis_note.txt):
    sub-01 ses-09 and ses-10 both have retfixRWblock01 and retfixRWblock02 in the
    same session. The decision was to rename retfixRWblock02_run-01 →
    retfixRWblock01_run-02, so downstream tools see only one task label (block01).

State confirmed by inspection (2026-04-16):
    ses-09 : PARTIALLY done — block01_run-02 already exists (renamed copy), but
             block02_run-01 was never removed from BIDS/func, BIDS/fmap,
             fmriprep/func, or fmriprep/fmap.
    ses-10 : Fully clean — block01_run-01 and block01_run-02 only in all locations.
             Script confirms this and skips gracefully.

Per-session rename rules (different for each session):
    ses-09 : block02_run-01 → block01_run-01  (task renamed; run number unchanged)
    ses-10 : block02_run-01 → block01_run-02  (task renamed; run-01 → run-02)

Actions per session:
    1. BIDS/func         — rename matching files in-place
    2. BIDS/fmap         — replace old stem with new stem in IntendedFor
    3. fmriprep/func     — rename matching files in-place
    4. fmriprep/fmap     — replace old stem with new stem in IntendedFor
    5. sourcedata        — rename matching _params.mat files in-place

Rename is atomic: src → .tmp_<uuid>/src.name → dst, no backup dirs created.
JSON patch is atomic: write to .tmp_<uuid>.json, then rename over original.

Usage:
    python 01b_special_fix_sub01_block02_rename.py                  # dry-run
    python 01b_special_fix_sub01_block02_rename.py --no-dry-run     # apply
    python 01b_special_fix_sub01_block02_rename.py -b /other/BIDS   # different root
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.rule import Rule

console = Console()
app = typer.Typer(pretty_exceptions_show_locals=False)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_DEFAULT_BIDS = Path("/scratch/tlei/VOTCLOC/BIDS")
_FMRIPREP_REL = Path("derivatives/fmriprep/analysis-25.1.4_t2w_fmapsbref_newest")

SUB      = "01"

# Per-session rename mapping — stems differ between sessions
# ses-09 : block02_run-01  →  block01_run-01  (task renamed, run number unchanged)
# ses-10 : block02_run-01  →  block01_run-02  (task renamed, run number also changed)
SESSION_MAP: dict[str, tuple[str, str]] = {
    "09": ("task-retfixRWblock02_run-01", "task-retfixRWblock01_run-01"),
    "10": ("task-retfixRWblock02_run-01", "task-retfixRWblock01_run-02"),
}
SESSIONS = list(SESSION_MAP)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _atomic_rename_file(src: Path, old_stem: str, new_stem: str, dry_run: bool) -> None:
    """Rename src in-place substituting old_stem → new_stem in the filename.

    Atomicity:
        1. src  →  .tmp_<uuid>/<src.name>   (atomic on POSIX same-FS)
        2. .tmp_<uuid>/<src.name>  →  dst   (atomic on POSIX same-FS)
    """
    new_name = src.name.replace(old_stem, new_stem)
    dst = src.parent / new_name
    console.print(f"    rename  [yellow]{src.name}[/yellow]")
    console.print(f"        →   [green]{new_name}[/green]")
    if not dry_run:
        tmp_dir = src.parent / f".tmp_{uuid.uuid4().hex}"
        tmp_dir.mkdir()
        try:
            tmp_path = tmp_dir / src.name
            src.rename(tmp_path)     # step 1
            tmp_path.rename(dst)     # step 2
        finally:
            if tmp_dir.exists():
                tmp_dir.rmdir()


def _patch_intendedfor(json_path: Path, old_stem: str, new_stem: str, dry_run: bool) -> int:
    """Replace old_stem with new_stem in IntendedFor entries.

    JSON write is atomic: write to .tmp_<uuid>.json, then rename over original.
    Returns number of entries patched.
    """
    data = json.loads(json_path.read_text())
    intended = data.get("IntendedFor", [])
    to_patch = [e for e in intended if old_stem in e]
    if not to_patch:
        return 0

    console.print(f"    patch  [cyan]{json_path.name}[/cyan]  ({len(to_patch)} entr(ies))")
    for e in to_patch:
        console.print(f"      [dim]{e}[/dim]")
        console.print(f"        →  [green]{e.replace(old_stem, new_stem)}[/green]")

    if not dry_run:
        data["IntendedFor"] = [
            e.replace(old_stem, new_stem) if old_stem in e else e
            for e in intended
        ]
        tmp_path = json_path.parent / f".tmp_{uuid.uuid4().hex}.json"
        try:
            tmp_path.write_text(json.dumps(data, indent=4))
            tmp_path.rename(json_path)   # atomic
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

    return len(to_patch)


# ---------------------------------------------------------------------------
# Per-session fix
# ---------------------------------------------------------------------------

def _fix_session(ses: str, old_stem: str, new_stem: str, bids: Path, fmriprep: Path, dry_run: bool) -> dict:
    counts = {"bids_func": 0, "bids_fmap": 0, "fp_func": 0, "fp_fmap": 0, "sourcedata": 0}

    console.print(Rule(f"sub-{SUB}  ses-{ses}  |  [yellow]{old_stem}[/yellow] → [green]{new_stem}[/green]", style="bold"))

    # 1. BIDS/func -----------------------------------------------------------
    bids_func = bids / f"sub-{SUB}" / f"ses-{ses}" / "func"
    console.print(f"\n[bold][1][/bold] BIDS/func: [dim]{bids_func}[/dim]")
    found = sorted(bids_func.glob(f"*{old_stem}*")) if bids_func.exists() else []
    if found:
        for f in found:
            _atomic_rename_file(f, old_stem, new_stem, dry_run)
        counts["bids_func"] = len(found)
    else:
        console.print("    [green]✓ clean[/green] — no matching files")

    # 2. BIDS/fmap -----------------------------------------------------------
    bids_fmap = bids / f"sub-{SUB}" / f"ses-{ses}" / "fmap"
    console.print(f"\n[bold][2][/bold] BIDS/fmap: [dim]{bids_fmap}[/dim]")
    n = 0
    if bids_fmap.exists():
        for j in sorted(bids_fmap.glob("*.json")):
            n += _patch_intendedfor(j, old_stem, new_stem, dry_run)
    if n == 0:
        console.print("    [green]✓ clean[/green] — no matching IntendedFor entries")
    counts["bids_fmap"] = n

    # 3. fmriprep/func -------------------------------------------------------
    fp_func = fmriprep / f"sub-{SUB}" / f"ses-{ses}" / "func"
    console.print(f"\n[bold][3][/bold] fmriprep/func: [dim]{fp_func}[/dim]")
    found = sorted(fp_func.glob(f"*{old_stem}*")) if fp_func.exists() else []
    if found:
        for f in found:
            _atomic_rename_file(f, old_stem, new_stem, dry_run)
        counts["fp_func"] = len(found)
    else:
        console.print("    [green]✓ clean[/green] — no matching files")

    # 4. fmriprep/fmap -------------------------------------------------------
    fp_fmap = fmriprep / f"sub-{SUB}" / f"ses-{ses}" / "fmap"
    console.print(f"\n[bold][4][/bold] fmriprep/fmap: [dim]{fp_fmap}[/dim]")
    n = 0
    if fp_fmap.exists():
        for j in sorted(fp_fmap.glob("*.json")):
            n += _patch_intendedfor(j, old_stem, new_stem, dry_run)
    if n == 0:
        console.print("    [green]✓ clean[/green] — no matching IntendedFor entries")
    counts["fp_fmap"] = n

    # 5. sourcedata ----------------------------------------------------------
    src_dir = bids / "sourcedata" / f"sub-{SUB}" / f"ses-{ses}"
    console.print(f"\n[bold][5][/bold] sourcedata: [dim]{src_dir}[/dim]")
    found = sorted(src_dir.glob(f"*{old_stem}*")) if src_dir.exists() else []
    if found:
        for f in found:
            _atomic_rename_file(f, old_stem, new_stem, dry_run)
        counts["sourcedata"] = len(found)
    else:
        console.print("    [green]✓ clean[/green] — no matching files")

    return counts


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

@app.command()
def main(
    bids_dir: Path = typer.Option(
        _DEFAULT_BIDS, "--bids", "-b", help="BIDS root directory."
    ),
    dry_run: bool = typer.Option(
        True,
        "--dry-run/--no-dry-run",
        help="Preview only (default). Use --no-dry-run to apply renames.",
    ),
) -> None:
    """Rename retfixRWblock02_run-01 → retfixRWblock01_run-02 for sub-01 ses-09/10."""

    fmriprep = bids_dir / _FMRIPREP_REL
    mode = "[yellow]DRY-RUN[/yellow]" if dry_run else "[red]APPLY[/red]"

    console.print(Rule(style="bold"))
    console.print(f"Special fix: sub-{SUB}  |  mode: {mode}")
    for ses, (old, new) in SESSION_MAP.items():
        console.print(f"  ses-{ses}:  [dim]{old}[/dim]  →  [green]{new}[/green]")
    console.print(f"BIDS:     {bids_dir}")
    console.print(f"fmriprep: {fmriprep}")
    console.print(Rule(style="bold"))

    totals = {"bids_func": 0, "bids_fmap": 0, "fp_func": 0, "fp_fmap": 0, "sourcedata": 0}
    for ses, (old_stem, new_stem) in SESSION_MAP.items():
        c = _fix_session(ses, old_stem, new_stem, bids_dir, fmriprep, dry_run)
        for k in totals:
            totals[k] += c[k]

    console.print(Rule(style="bold"))
    console.print("[bold]Summary:[/bold]")
    console.print(f"  BIDS/func      files renamed : {totals['bids_func']}")
    console.print(f"  BIDS/fmap      entries patched: {totals['bids_fmap']}")
    console.print(f"  fmriprep/func  files renamed : {totals['fp_func']}")
    console.print(f"  fmriprep/fmap  entries patched: {totals['fp_fmap']}")
    console.print(f"  sourcedata     files renamed : {totals['sourcedata']}")
    if dry_run:
        console.print("\nRe-run with [bold]--no-dry-run[/bold] to apply.")
    console.print(Rule(style="bold"))


if __name__ == "__main__":
    app()
