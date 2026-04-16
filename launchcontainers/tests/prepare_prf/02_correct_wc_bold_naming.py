"""
02_correct_wc_bold_naming.py
-----------------
For WC sessions: match vistadisplog *_params.mat files to BIDS func bold/sbref
by AcquisitionTime, check whether task/run labels agree, and rename BIDS files
to match the params.mat task label when they don't.

How matching works
------------------
1. Read all *_params.mat files from
   BIDS/sourcedata/vistadisplog/sub-XX/ses-XX/
   Each _params.mat is a symlink → the actual log file is a timestamp .mat.
   The BIDS-task/run label is in the _params.mat filename itself.
   The AcquisitionTime comes from the symlink target filename (timestamp).

2. Read all retfix* bold.json sidecars from BIDS/sub-XX/ses-XX/func/.
   Pull AcquisitionTime from the JSON.

3. Match by AcquisitionTime (within max_diff_sec, default 30 s).

4. For each matched pair: if the task-XX_run-YY in the BIDS filename differs
   from the task-XX_run-YY in the params.mat filename → rename needed.

5. Rename: copy all 4 files (bold.nii.gz, bold.json, sbref.nii.gz, sbref.json)
   to a tmp folder with new names, then move back to func/.
   Dry-run mode (default) prints what would happen without touching files.

Usage
-----
    # check only — dry run (default)
    python 02_correct_wc_bold_naming.py --bidsdir /path/BIDS -s 06,10

    # batch from WC subseslist
    python 02_correct_wc_bold_naming    .py --bidsdir /path/BIDS -f wc_subseslist.txt

    # actually rename (parallel)
    python 02_correct_wc_bold_naming.py --bidsdir /path/BIDS -f wc_subseslist.txt \\
        --no-dry-run -w 8
"""

from __future__ import annotations

import glob
import os
import os.path as op
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from launchcontainers.utils import (
    atomic_rename_pairs,
    hms_to_sec,
    parse_hms,
    parse_subses_list,
    read_json_acqtime,
)

console = Console()
app = typer.Typer(pretty_exceptions_show_locals=False)

# Expected offset range: params.mat is written ~5-6 min after scan start
MAT_BIDS_OFFSET_MIN = 280  # seconds
MAT_BIDS_OFFSET_MAX = 590  # seconds

# Extensions that travel together for every bold/sbref pair
_BOLD_EXTS = ["_bold.nii.gz", "_bold.json"]
_SBREF_EXTS = ["_sbref.nii.gz", "_sbref.json"]
_ALL_EXTS = _BOLD_EXTS + _SBREF_EXTS


# ---------------------------------------------------------------------------
# Core: build match table for one session
# ---------------------------------------------------------------------------


def _read_params_mats(bidsdir: str, sub: str, ses: str) -> list[dict]:
    """
    Read *_params.mat files from vistadisplog.
    Returns list of:
        {mat_name, mat_task, mat_run, mat_acq_time}
    mat_acq_time is parsed from the symlink target filename (timestamp .mat).
    """
    displog_dir = op.join(
        bidsdir, "sourcedata", "vistadisplog", f"sub-{sub}", f"ses-{ses}"
    )
    rows = []
    for mat in sorted(glob.glob(op.join(displog_dir, "*_params.mat"))):
        mat_name = op.basename(mat)
        m = re.search(r"task-(\w+)_run-(\d+)", mat_name)
        if not m:
            continue
        mat_task, mat_run = m.group(1), m.group(2)

        # AcquisitionTime from symlink target (timestamp filename)
        acq_time = ""
        try:
            if os.path.islink(mat):
                target = os.readlink(mat)  # e.g. 20230531T172442.mat
                stem = Path(target).stem  # 20230531T172442
                # format: YYYYMMDDTHHMMSS
                ts_m = re.search(r"T(\d{2})(\d{2})(\d{2})$", stem)
                if ts_m:
                    acq_time = f"{ts_m.group(1)}:{ts_m.group(2)}:{ts_m.group(3)}"
        except Exception:
            pass

        rows.append(
            {
                "mat_name": mat_name,
                "mat_task": mat_task,
                "mat_run": mat_run,
                "mat_acq_time": acq_time,
            }
        )
    rows.sort(key=lambda r: hms_to_sec(r["mat_acq_time"]))
    return rows


def _read_bids_bold(bidsdir: str, sub: str, ses: str) -> list[dict]:
    """
    Read retfix* bold.json sidecars from BIDS func.
    Returns list of:
        {bids_name_stem, bids_task, bids_run, bids_acq_time}
    bids_name_stem = everything before _bold.json suffix.
    """
    func_dir = op.join(bidsdir, f"sub-{sub}", f"ses-{ses}", "func")
    rows = []
    pattern = op.join(func_dir, f"sub-{sub}_ses-{ses}_task-ret*_run-*_bold.json")
    for jf in sorted(glob.glob(pattern)):
        basename = op.basename(jf)
        m = re.search(r"task-(\w+)_run-(\d+)", basename)
        if not m:
            continue
        bids_task, bids_run = m.group(1), m.group(2)
        acq_time = parse_hms(read_json_acqtime(jf))
        stem = basename.replace("_bold.json", "")
        rows.append(
            {
                "bids_stem": stem,
                "bids_task": bids_task,
                "bids_run": bids_run,
                "bids_acq_time": acq_time,
            }
        )
    rows.sort(key=lambda r: hms_to_sec(r["bids_acq_time"]))
    return rows


def build_match_table(
    bidsdir: str,
    sub: str,
    ses: str,
    offset_min: int = MAT_BIDS_OFFSET_MIN,
    offset_max: int = MAT_BIDS_OFFSET_MAX,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Match params.mat entries to BIDS bold entries by positional order.

    Both lists are sorted by their respective AcquisitionTime, then matched
    1st-to-1st, 2nd-to-2nd, etc.  This is correct because:
      - params.mat timestamp = vistadisp log written at *end* of run (~5-6 min
        after scan start), so absolute time difference is too large for
        proximity matching.
      - Temporal *order* is preserved across both lists.

    Returns
    -------
    matched        : list of dicts — one per matched pair
    unmatched_mat  : leftover params.mat entries (more mats than bolds)
    unmatched_bids : leftover BIDS entries (more bolds than mats)
    """
    mat_rows = _read_params_mats(bidsdir, sub, ses)
    bids_rows = _read_bids_bold(bidsdir, sub, ses)

    n = min(len(mat_rows), len(bids_rows))
    matched: list[dict] = []

    for i in range(n):
        mr = mat_rows[i]
        br = bids_rows[i]
        mat_sec = hms_to_sec(mr["mat_acq_time"])
        bids_sec = hms_to_sec(br["bids_acq_time"])
        diff = round(mat_sec - bids_sec, 2)  # positive = mat is later (expected)
        offset_ok = offset_min <= diff <= offset_max
        label_match = (
            mr["mat_task"] == br["bids_task"] and mr["mat_run"] == br["bids_run"]
        )
        matched.append(
            {
                "mat_name": mr["mat_name"],
                "mat_task": mr["mat_task"],
                "mat_run": mr["mat_run"],
                "mat_acq_time": mr["mat_acq_time"],
                "bids_stem": br["bids_stem"],
                "bids_task": br["bids_task"],
                "bids_run": br["bids_run"],
                "bids_acq_time": br["bids_acq_time"],
                "diff_sec": diff,  # mat − bids; expect 300–400 s
                "offset_ok": offset_ok,
                "label_match": label_match,
            }
        )

    unmatched_mat = mat_rows[n:]
    unmatched_bids = bids_rows[n:]
    return matched, unmatched_mat, unmatched_bids


# ---------------------------------------------------------------------------
# Per-session check + fix
# ---------------------------------------------------------------------------


def check_and_fix_session(
    bidsdir: str,
    sub: str,
    ses: str,
    dry_run: bool = True,
    offset_min: int = MAT_BIDS_OFFSET_MIN,
    offset_max: int = MAT_BIDS_OFFSET_MAX,
) -> dict:
    """
    Build match table, print summary, rename mismatched files if not dry_run.
    Returns a result dict with keys: sub, ses, matched, renamed, warnings.
    """
    func_dir = op.join(bidsdir, f"sub-{sub}", f"ses-{ses}", "func")
    matched, unmatched_mat, unmatched_bids = build_match_table(
        bidsdir, sub, ses, offset_min=offset_min, offset_max=offset_max
    )

    renamed: list[str] = []
    warnings: list[str] = []

    console.print(f"\n[bold cyan]sub-{sub}  ses-{ses}[/]")

    if not matched and not unmatched_mat and not unmatched_bids:
        console.print("  [yellow]No params.mat or bold files found — skipping.[/]")
        return {
            "sub": sub,
            "ses": ses,
            "matched": 0,
            "renamed": 0,
            "warnings": ["no data"],
        }

    # Print match table
    t = Table(show_header=True, header_style="bold magenta", box=None)
    t.add_column("mat_task_run", style="dim")
    t.add_column("mat_acq")
    t.add_column("bids_task_run", style="dim")
    t.add_column("bids_acq")
    t.add_column("mat−bids(s)", justify="right")
    t.add_column("offset", justify="center")
    t.add_column("label", justify="center")

    for m in matched:
        label_str = "[green]OK[/]" if m["label_match"] else "[red]MISMATCH[/]"
        offset_str = "[green]OK[/]" if m["offset_ok"] else "[red]SUSPICIOUS[/]"
        t.add_row(
            f"task-{m['mat_task']}_run-{m['mat_run']}",
            m["mat_acq_time"],
            f"task-{m['bids_task']}_run-{m['bids_run']}",
            m["bids_acq_time"],
            str(m["diff_sec"]),
            offset_str,
            label_str,
        )
    console.print(t)

    for um in unmatched_mat:
        msg = f"  [yellow]⚠ params.mat unmatched:[/] {um['mat_name']} (acq={um['mat_acq_time']})"
        console.print(msg)
        warnings.append(f"unmatched mat: {um['mat_name']}")

    for ub in unmatched_bids:
        msg = f"  [yellow]⚠ BIDS bold unmatched:[/] {ub['bids_stem']} (acq={ub['bids_acq_time']})"
        console.print(msg)
        warnings.append(f"unmatched bids: {ub['bids_stem']}")

    # Collect all rename pairs for this session, then execute atomically in one
    # batched call. This handles swaps (e.g. retCB_run-01 ↔ retFF_run-02)
    # correctly: phase 1 parks all sources as tmp names before phase 2 places
    # anything, so no destination can be clobbered by a sibling rename.
    all_pairs: list[tuple[Path, Path]] = []
    stem_map: list[tuple[str, str]] = []  # for display only

    for m in matched:
        if m["label_match"]:
            continue
        if not m["offset_ok"]:
            console.print(
                f"  [red]⚠ SKIP rename[/] — offset {m['diff_sec']}s outside "
                f"[{MAT_BIDS_OFFSET_MIN},{MAT_BIDS_OFFSET_MAX}]s: "
                f"task-{m['bids_task']}_run-{m['bids_run']} (suspicious pairing)"
            )
            warnings.append(f"suspicious offset {m['diff_sec']}s: {m['bids_stem']}")
            continue
        old_stem = m["bids_stem"]
        new_stem = old_stem.replace(
            f"task-{m['bids_task']}_run-{m['bids_run']}",
            f"task-{m['mat_task']}_run-{m['mat_run']}",
        )
        stem_map.append((old_stem, new_stem))
        for ext in _ALL_EXTS:
            src = Path(op.join(func_dir, old_stem + ext))
            dst = Path(op.join(func_dir, new_stem + ext))
            if src.exists():
                all_pairs.append((src, dst))

    tag = "[dim][DRY][/]" if dry_run else "[green][RENAME][/]"
    for old_stem, new_stem in stem_map:
        console.print(f"  {tag} {old_stem} → {new_stem}")

    if all_pairs:
        try:
            atomic_rename_pairs(all_pairs, dry_run=dry_run)
            renamed = [f"{s.name} → {d.name}" for s, d in all_pairs]
        except RuntimeError as exc:
            console.print(f"  [red]ERROR[/] {exc}")
            warnings.append(str(exc))

    return {
        "sub": sub,
        "ses": ses,
        "matched": len(matched),
        "renamed": len(renamed),
        "warnings": warnings,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@app.command()
def main(
    bidsdir: Path = typer.Option(..., "--bidsdir", "-b", help="BIDS root directory."),
    subses: Optional[str] = typer.Option(
        None,
        "--subses",
        "-s",
        help="Single sub,ses pair e.g. 06,10",
    ),
    subses_file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-f",
        help="Subseslist file (TSV/CSV/TXT with sub,ses columns).",
    ),
    execute: bool = typer.Option(
        False,
        "--execute",
        help="Actually rename files. Without this flag the script always runs as dry-run.",
    ),
    workers: Optional[int] = typer.Option(
        None,
        "--workers",
        "-w",
        help="Parallel workers. Omit for serial.",
    ),
    offset_min: int = typer.Option(
        MAT_BIDS_OFFSET_MIN,
        "--offset-min",
        help="Min expected mat−bids offset in seconds (default 300).",
    ),
    offset_max: int = typer.Option(
        MAT_BIDS_OFFSET_MAX,
        "--offset-max",
        help="Max expected mat−bids offset in seconds (default 550).",
    ),
):
    """
    Match vistadisplog params.mat to BIDS bold/sbref by AcquisitionTime and rename if needed.

    Always runs a dry-run first (printed to console). Pass --execute to apply the renames.
    """
    if subses_file is not None:
        pairs = parse_subses_list(subses_file)
        console.print(f"[dim]Loaded {len(pairs)} session(s) from {subses_file.name}[/]")
    elif subses is not None:
        parts = [p.strip().zfill(2) for p in subses.split(",")]
        if len(parts) != 2:
            console.print("[red]--subses must be sub,ses e.g. 06,10[/]")
            raise typer.Exit(1)
        pairs = [(parts[0], parts[1])]
    else:
        console.print("[red]Provide --subses or --file.[/]")
        raise typer.Exit(1)

    def _run_pass(dry_run: bool) -> list[dict]:
        results: list[dict] = []
        if workers:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(
                        check_and_fix_session,
                        str(bidsdir),
                        sub,
                        ses,
                        dry_run,
                        offset_min,
                        offset_max,
                    ): (sub, ses)
                    for sub, ses in pairs
                }
                for fut in as_completed(futures):
                    try:
                        results.append(fut.result())
                    except Exception as exc:
                        s, e = futures[fut]
                        console.print(f"  [red]ERROR[/] sub-{s} ses-{e}: {exc}")
        else:
            for sub, ses in pairs:
                try:
                    results.append(
                        check_and_fix_session(
                            str(bidsdir), sub, ses, dry_run, offset_min, offset_max
                        )
                    )
                except Exception as exc:
                    console.print(f"  [red]ERROR[/] sub-{sub} ses-{ses}: {exc}")
        results.sort(key=lambda r: (r["sub"], r["ses"]))
        return results

    # ── Always run dry-run first ──────────────────────────────────────────────
    console.print("\n[bold yellow]─── DRY-RUN ─── (no files changed)[/bold yellow]")
    dry_results = _run_pass(dry_run=True)
    n_mismatches = sum(r["renamed"] for r in dry_results)

    total_warn = sum(len(r["warnings"]) for r in dry_results)
    console.print(
        f"\n[bold]Dry-run summary:[/] {len(dry_results)} session(s) — "
        f"{n_mismatches} file rename(s) needed, {total_warn} warning(s)"
    )

    if not execute:
        console.print(
            "\n[dim]Pass [bold]--execute[/bold] to apply the renames above.[/dim]"
        )
        return

    if n_mismatches == 0:
        console.print("\n[green]Nothing to rename — all labels already match.[/green]")
        return

    # ── Execute pass ─────────────────────────────────────────────────────────
    console.print("\n[bold green]─── EXECUTE ─── (renaming files)[/bold green]")
    exec_results = _run_pass(dry_run=False)
    total_renamed = sum(r["renamed"] for r in exec_results)
    console.print(
        f"\n[bold green]Done:[/bold green] {total_renamed} file(s) renamed across "
        f"{len(exec_results)} session(s)."
    )


if __name__ == "__main__":
    app()
