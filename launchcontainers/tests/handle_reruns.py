"""
handle_reruns.py
----------------
Check and fix extra (rerun) BOLD acquisitions for the VOTCLOC experiment.

Background
----------
When a subject has poor performance during a run, an extra run is acquired at
the end of the session to compensate.  The lab note records these in
``protocol_name`` with a format like::

    fLoc_run-11_rerun-04

meaning run-11 is the extra acquisition that compensates for run-04.

Sub-commands
------------
**check**  — compare lab note reruns vs BIDS extra runs
**fix**    — apply corrections (dry-run unless --execute)
**parse**  — print all rerun rows parsed from the lab note

CLI examples
------------
Single session::

    python handle_reruns.py check \\
        --lab-note /path/to/xlsx --bidsdir /path/to/BIDS -s 11,10

Batch from subseslist::

    python handle_reruns.py check \\
        --lab-note /path/to/xlsx --bidsdir /path/to/BIDS -f subseslist.tsv

Fix (dry-run)::

    python handle_reruns.py fix \\
        --lab-note /path/to/xlsx --bidsdir /path/to/BIDS -s 11,10

Fix (execute)::

    python handle_reruns.py fix \\
        --lab-note /path/to/xlsx --bidsdir /path/to/BIDS -f subseslist.tsv --execute
"""

from __future__ import annotations

import csv
import fnmatch
import glob
import json
import os
import os.path as op
import re
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

# Import rename helpers from the sibling package (no __init__.py, use path injection)
sys.path.insert(0, str(Path(__file__).parent / "renaming_ret_bold"))
from correct_wc_bold_naming import check_and_fix_session  # noqa: E402

console = Console()
app = typer.Typer(pretty_exceptions_show_locals=False)

# ---------------------------------------------------------------------------
# Constants — mirrors BIDSfuncSpec
# ---------------------------------------------------------------------------

FLOC_STANDARD_RUNS: set[str] = {f"{i:02d}" for i in range(1, 11)}  # 01–10
RET_STANDARD_RUNS: set[str] = {"01", "02"}
RET_TASKS: set[str] = {"retRW", "retFF", "retCB"}
TASKS_OF_INTEREST: list[str] = ["fLoc", "ret*"]  # shell-wildcard patterns


def _is_task_of_interest(task: str) -> bool:
    return any(fnmatch.fnmatch(task, pat) for pat in TASKS_OF_INTEREST)


def _is_standard_run(task: str, run: str) -> bool:
    """True if (task, run) belongs to the expected 16-group set."""
    if task == "fLoc":
        return run in FLOC_STANDARD_RUNS
    if task in RET_TASKS:
        return run in RET_STANDARD_RUNS
    return False


# ---------------------------------------------------------------------------
# Lab-note parser
# ---------------------------------------------------------------------------

_BAD_SES_PATTERN = r"-|wrong|failed|lost|ME|bad|00|test|-t"


def _safe_zfill(v) -> str:
    """Zero-pad a single sub/ses value; return non-numeric values as-is."""
    s = str(v).strip()
    try:
        return str(int(float(s))).zfill(2)
    except (ValueError, TypeError):
        return s


def _clean_subses_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df[["sub", "ses"]] = df[["sub", "ses"]].replace("", pd.NA).ffill()
    df = df.dropna(subset=["sub", "ses"])
    df["sub"] = df["sub"].apply(_safe_zfill)
    df["ses"] = df["ses"].apply(_safe_zfill)
    df = df[~df["ses"].str.contains(_BAD_SES_PATTERN, na=False)]
    return df


def _extract_rerun_rows(df: pd.DataFrame) -> list[dict]:
    rows = []
    rerun_df = df[
        df["protocol_name"].str.contains("rerun", case=False, na=False)
        & ~df["protocol_name"].str.contains(r"qmri|T1", case=False, na=False)
    ]
    for _, row in rerun_df.iterrows():
        proto = str(row["protocol_name"]).strip()
        parts = proto.split("_")

        run_part = next((p for p in parts if re.match(r"run-\d+$", p, re.I)), None)
        rerun_part = next((p for p in parts if re.search(r"rerun-?\d+", p, re.I)), None)

        task = None
        if any("fLoc" in p for p in parts):
            task = "fLoc"
        else:
            task_hit = next((p for p in parts if re.match(r"ret\w+", p, re.I)), None)
            if task_hit:
                task = task_hit

        if run_part and rerun_part and task:
            extra_run = run_part.split("-")[-1].zfill(2)
            compensates_run = re.search(r"\d+", rerun_part).group(0).zfill(2)
            rows.append(
                {
                    "sub": str(row["sub"]),
                    "ses": str(row["ses"]),
                    "task": task,
                    "extra_run": extra_run,
                    "compensates_run": compensates_run,
                    "protocol_name": proto,
                }
            )
    return rows


def parse_lab_note(lab_note_path: Path) -> pd.DataFrame:
    """
    Read the lab note (.xlsx or flat .tsv/.csv) and return all rerun rows.

    Columns: ``sub``, ``ses``, ``task``, ``extra_run``,
    ``compensates_run``, ``protocol_name``.
    """
    ext = lab_note_path.suffix.lower()
    rows: list[dict] = []

    if ext in (".xlsx", ".xls"):
        xls = pd.ExcelFile(lab_note_path)
        for sheet in xls.sheet_names:
            if not sheet.startswith("sub-"):
                continue
            df = pd.read_excel(xls, sheet_name=sheet, header=0)
            needed = [c for c in ["sub", "ses", "protocol_name"] if c in df.columns]
            if len(needed) < 3:
                continue
            df = df[needed].copy()
            df = _clean_subses_df(df)
            rows.extend(_extract_rerun_rows(df))
    else:
        delimiter = "\t" if ext == ".tsv" else ","
        df = pd.read_csv(lab_note_path, sep=delimiter, dtype=str)
        df.columns = [c.strip().lower() for c in df.columns]
        needed = [c for c in ["sub", "ses", "protocol_name"] if c in df.columns]
        if len(needed) < 3:
            raise ValueError(
                f"Lab note must have columns 'sub', 'ses', 'protocol_name'. "
                f"Found: {list(df.columns)}"
            )
        df = df[needed].copy()
        df = _clean_subses_df(df)
        rows.extend(_extract_rerun_rows(df))

    if not rows:
        return pd.DataFrame(
            columns=[
                "sub",
                "ses",
                "task",
                "extra_run",
                "compensates_run",
                "protocol_name",
            ]
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# BIDS scanning helpers
# ---------------------------------------------------------------------------


def _bids_extras_for_session(
    bidsdir: str, sub: str, ses: str
) -> tuple[list[tuple[str, str]], bool]:
    """
    Return (extras, func_exists) where:
    * extras      — (task, run) pairs beyond the standard 16 groups
    * func_exists — False when the BIDS func folder is absent
    """
    func_dir = op.join(bidsdir, f"sub-{sub}", f"ses-{ses}", "func")
    if not op.isdir(func_dir):
        return [], False

    extras = []
    for bold in sorted(
        glob.glob(op.join(func_dir, f"sub-{sub}_ses-{ses}_task-*_run-*_bold.nii.gz"))
    ):
        m = re.search(r"task-(\w+)_run-(\d+)", op.basename(bold))
        if not m:
            continue
        task, run = m.group(1), m.group(2)
        if not _is_task_of_interest(task):
            continue
        if not _is_standard_run(task, run):
            extras.append((task, run))
    return extras, True


def _load_mapping_tsv(bidsdir: str, sub: str, ses: str) -> list[dict]:
    tsv = op.join(
        bidsdir,
        "sourcedata",
        "vistadisplog",
        f"sub-{sub}",
        f"ses-{ses}",
        f"sub-{sub}_ses-{ses}_desc-mapping_PRF_acqtime.tsv",
    )
    if not op.exists(tsv):
        return []
    with open(tsv, newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def _mat_file_for_task_run(
    bidsdir: str, sub: str, ses: str, task: str, run: str
) -> str | None:
    mapping = _load_mapping_tsv(bidsdir, sub, ses)
    target = f"task-{task}_run-{run}"
    for row in mapping:
        if row.get("task_run") == target:
            return op.join(row["log_file_path"], row["log_file_name"])
    return None


# ---------------------------------------------------------------------------
# Core: check a single session
# ---------------------------------------------------------------------------


def check_single_session(
    bidsdir: str,
    sub: str,
    ses: str,
    df_note: pd.DataFrame,
) -> dict:
    """
    Compare lab note reruns with BIDS extra runs for one sub/ses.

    Returns
    -------
    dict with keys:
        sub, ses,
        note_rows         — matching lab note DataFrame slice
        bids_extras       — list of (task, run) found beyond standard in BIDS
        note_set          — set of (task, extra_run) from lab note
        bids_set          — set of (task, run) from BIDS extras
        matched           — in both
        only_note_extra   — in lab note with extra_run > standard, BUT not in BIDS
        only_bids         — in BIDS but not in lab note
        within_range      — lab note reruns where extra_run is within standard range
                            (cannot be detected just from run counts)
        status            — "OK" | "MISMATCH" | "WITHIN_RANGE_ONLY"
    """
    note_rows = df_note[(df_note["sub"] == sub) & (df_note["ses"] == ses)]
    bids_extras, func_exists = _bids_extras_for_session(bidsdir, sub, ses)

    if not func_exists:
        return {
            "sub": sub,
            "ses": ses,
            "bidsdir": bidsdir,
            "note_rows": note_rows,
            "bids_extras": [],
            "note_set": set(),
            "bids_set": set(),
            "matched": set(),
            "only_note_extra": set(),
            "only_bids": set(),
            "within_range": set(),
            "status": "NO_FUNC_DIR",
        }

    bids_set = set(bids_extras)

    # Split lab note rows into truly-extra vs within-range
    within_range: set[tuple[str, str]] = set()
    note_set: set[tuple[str, str]] = set()

    for _, nr in note_rows.iterrows():
        key = (nr.task, nr.extra_run)
        if _is_standard_run(nr.task, nr.extra_run):
            within_range.add(key)
        else:
            note_set.add(key)

    matched = note_set & bids_set
    only_note_extra = note_set - bids_set  # truly extra in lab note but missing in BIDS
    only_bids = bids_set - note_set  # in BIDS but not documented in lab note

    if only_note_extra or only_bids:
        status = "MISMATCH"
    elif within_range and not note_set and not bids_set:
        status = "WITHIN_RANGE_ONLY"
    else:
        status = "OK"

    return {
        "sub": sub,
        "ses": ses,
        "note_rows": note_rows,
        "bids_extras": bids_extras,
        "note_set": note_set,
        "bids_set": bids_set,
        "matched": matched,
        "only_note_extra": only_note_extra,
        "only_bids": only_bids,
        "within_range": within_range,
        "status": status,
    }


def _print_session_result(r: dict) -> None:
    """Print a rich summary for one session check result."""
    colour = {
        "OK": "green",
        "WITHIN_RANGE_ONLY": "cyan",
        "MISMATCH": "bold red",
        "NO_FUNC_DIR": "yellow",
    }.get(r["status"], "white")
    console.print(
        f"\n[bold]sub-{r['sub']}  ses-{r['ses']}[/]  "
        f"status=[{colour}]{r['status']}[/]  "
        f"note_truly_extra={len(r['note_set'])}  "
        f"bids_extra={len(r['bids_set'])}  "
        f"within_range={len(r['within_range'])}"
    )
    if r["status"] == "NO_FUNC_DIR":
        missing = op.join(
            str(r.get("bidsdir", "")),
            f"sub-{r['sub']}",
            f"ses-{r['ses']}",
            "func",
        )
        console.print(f"  [yellow][WARN][/] BIDS func dir missing: {missing}")
    for task, run in sorted(r["matched"]):
        console.print(f"  [green]✓[/] matched     task-{task}_run-{run}")
    for task, run in sorted(r["only_note_extra"]):
        console.print(
            f"  [red]✗[/] note only   task-{task}_run-{run}  (not found in BIDS)"
        )
    for task, run in sorted(r["only_bids"]):
        console.print(
            f"  [red]✗[/] BIDS only   task-{task}_run-{run}  (not in lab note)"
        )
    for task, run in sorted(r["within_range"]):
        console.print(
            f"  [cyan]~[/] within-range task-{task}_run-{run}  "
            f"(run ≤ standard max — cannot verify from count)"
        )


# ---------------------------------------------------------------------------
# Fix helpers
# ---------------------------------------------------------------------------


def _fix_floc(
    bidsdir: str,
    sub: str,
    ses: str,
    extra_run: str,
    bad_run: str,
    dry_run: bool,
) -> None:
    func_dir = op.join(bidsdir, f"sub-{sub}", f"ses-{ses}", "func")
    src = op.join(func_dir, f"sub-{sub}_ses-{ses}_task-fLoc_run-{extra_run}_events.tsv")
    dst = op.join(func_dir, f"sub-{sub}_ses-{ses}_task-fLoc_run-{bad_run}_events.tsv")

    if not op.exists(src):
        console.print(
            f"  [yellow][WARN][/] source events.tsv missing: {op.basename(src)}"
        )
        return

    if dry_run:
        console.print(
            f"  [dim][DRY][/] fLoc symlink\n"
            f"    {op.basename(src)}\n    → {op.basename(dst)}"
        )
        return

    if op.islink(dst) or op.exists(dst):
        os.remove(dst)
    os.symlink(src, dst)
    console.print(
        f"  [green]✓[/] fLoc symlink\n    {op.basename(src)}\n    → {op.basename(dst)}"
    )


def _fix_ret(
    bidsdir: str,
    sub: str,
    ses: str,
    task: str,
    bad_run: str,
    dry_run: bool,
) -> None:
    func_dir = op.join(bidsdir, f"sub-{sub}", f"ses-{ses}", "func")
    prefix = f"sub-{sub}_ses-{ses}_task-{task}_run-{bad_run}"
    bids_files = [
        f
        for f in sorted(glob.glob(op.join(func_dir, f"{prefix}*")))
        if f.endswith(".nii.gz") or f.endswith(".json")
    ]
    mat_file = _mat_file_for_task_run(bidsdir, sub, ses, task, bad_run)

    if dry_run:
        console.print(f"  [dim][DRY][/] {task} run-{bad_run}: would delete:")
        for f in bids_files:
            console.print(f"    BIDS : {op.basename(f)}")
        console.print(
            f"    .mat : {op.basename(mat_file) if mat_file else '[not found in mapping TSV]'}"
        )
        return

    deleted = []
    for f in bids_files:
        if op.exists(f) or op.islink(f):
            os.remove(f)
            deleted.append(op.basename(f))
    if mat_file and (op.exists(mat_file) or op.islink(mat_file)):
        os.remove(mat_file)
        deleted.append(f"[mat] {op.basename(mat_file)}")

    if deleted:
        console.print(
            f"  [green]✓[/] {task} run-{bad_run}: deleted {len(deleted)} file(s):"
        )
        for d in deleted:
            console.print(f"    {d}")
    else:
        console.print(
            f"  [yellow][WARN][/] {task} run-{bad_run}: no files found to delete."
        )


def fix_single_session(
    bidsdir: str,
    sub: str,
    ses: str,
    df_note: pd.DataFrame,
    dry_run: bool = True,
) -> None:
    """
    Apply rerun corrections for one sub/ses.

    * fLoc  — symlink events.tsv from the rerun slot to the bad original slot
    * ret*  — delete the poor-quality original run from BIDS func and sourcedata
    * Always writes (or dry-prints) a per-session JSON mapping file
    """
    note_rows = df_note[(df_note["sub"] == sub) & (df_note["ses"] == ses)]
    if note_rows.empty:
        console.print(
            f"  [yellow]No lab note reruns for sub-{sub} ses-{ses} — skipping.[/]"
        )
        return

    func_dir = op.join(bidsdir, f"sub-{sub}", f"ses-{ses}", "func")
    mapping_json_path = op.join(
        func_dir, f"sub-{sub}_ses-{ses}_desc-rerun_mapping.json"
    )

    entries = []
    for _, nr in note_rows.iterrows():
        entries.append(
            {
                "task": nr.task,
                "extra_run": f"run-{nr.extra_run}",
                "compensates_run": f"run-{nr.compensates_run}",
                "action": "symlink_events_tsv"
                if nr.task == "fLoc"
                else "delete_bad_run",
            }
        )
        if nr.task == "fLoc":
            _fix_floc(bidsdir, sub, ses, nr.extra_run, nr.compensates_run, dry_run)
        elif re.match(r"ret\w+", nr.task, re.I):
            _fix_ret(bidsdir, sub, ses, nr.task, nr.compensates_run, dry_run)
        else:
            console.print(f"  [yellow]Unknown task '{nr.task}' — skipping.[/]")

    payload = {"sub": sub, "ses": ses, "reruns": entries}
    if dry_run:
        console.print(
            f"  [dim][DRY][/] mapping JSON → {mapping_json_path}\n"
            f"  {json.dumps(payload, indent=2)}"
        )
    else:
        os.makedirs(func_dir, exist_ok=True)
        with open(mapping_json_path, "w") as fh:
            json.dump(payload, fh, indent=2)
        console.print(f"  [green]✓[/] mapping JSON → {mapping_json_path}")


# ---------------------------------------------------------------------------
# Helpers for reading checker output and subseslist
# ---------------------------------------------------------------------------


def _cross_check_subseslist_vs_labnote(
    pairs: list[tuple[str, str]],
    df_note: pd.DataFrame,
) -> None:
    """
    Compare (sub, ses) pairs from the subseslist against sessions found in
    the lab note that have rerun entries.  Print any discrepancies.
    """
    subses_set = {(s, e) for s, e in pairs}
    note_set = {(row["sub"], row["ses"]) for _, row in df_note.iterrows()}

    only_note = note_set - subses_set  # in lab note but not in subseslist
    only_subses = subses_set - note_set  # in subseslist but not in lab note
    both = subses_set & note_set

    console.print(
        f"\n[bold]Cross-check subseslist ↔ lab note[/]  "
        f"subseslist={len(subses_set)}  "
        f"lab_note={len(note_set)}  "
        f"matched={len(both)}"
    )

    if only_note:
        console.print("\n  [yellow]In lab note (rerun) but NOT in subseslist:[/]")
        for sub, ses in sorted(only_note):
            console.print(f"    sub-{sub}  ses-{ses}")

    if only_subses:
        console.print("\n  [yellow]In subseslist (RUN=False) but NOT in lab note:[/]")
        for sub, ses in sorted(only_subses):
            console.print(f"    sub-{sub}  ses-{ses}")

    if not only_note and not only_subses:
        console.print("  [green]✓ subseslist and lab note are in agreement.[/]")


def _pairs_from_subses_list(path: Path) -> list[tuple[str, str]]:
    ext = path.suffix.lower()
    delimiter = "\t" if ext == ".tsv" else ","
    pairs = []
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh, delimiter=delimiter)
        for row in reader:
            RUN = row.get("RUN", "").strip().replace("\r", "")
            if RUN and RUN != "False":
                continue
            pairs.append((str(row["sub"]).strip(), str(row["ses"]).strip()))
    return pairs


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------


def _resolve_pairs(
    subses: Optional[str], subses_file: Optional[Path], df_note: pd.DataFrame
) -> list[tuple[str, str]]:
    """
    Resolve sub/ses pairs from -s or -f options.
    Falls back to all sessions in the lab note when neither is given.
    """
    if subses_file is not None:
        pairs = _pairs_from_subses_list(subses_file)
        console.print(
            f"  [cyan]{len(pairs)} sub/ses pair(s) from {subses_file.name}.[/]"
        )
        return pairs
    if subses is not None:
        parts = [p.strip().zfill(2) for p in subses.split(",")]
        if len(parts) != 2:
            console.print("[red]--subses must be sub,ses e.g. 06,10[/]")
            raise typer.Exit(1)
        console.print(f"  [cyan]Single session: sub-{parts[0]}  ses-{parts[1]}.[/]")
        return [(parts[0], parts[1])]
    # fall back to all sessions in the lab note
    pairs = [
        tuple(r) for r in df_note[["sub", "ses"]].drop_duplicates().values.tolist()
    ]
    console.print(
        f"  [dim]No -s or -f provided; using all {len(pairs)} lab note session(s).[/]"
    )
    return pairs


@app.command("check")
def cmd_check(
    lab_note: Path = typer.Option(..., "--lab-note"),
    bidsdir: Path = typer.Option(..., "--bidsdir"),
    subses: Optional[str] = typer.Option(
        None, "--subses", "-s", help="Single sub,ses pair e.g. 06,10"
    ),
    subses_file: Optional[Path] = typer.Option(
        None, "--file", "-f", help="Subseslist TSV/CSV with sub and ses columns."
    ),
    output_tsv: Optional[Path] = typer.Option(
        None,
        "--output-tsv",
        help="Write report to this file. Defaults to <bidsdir>/sourcedata/qc/rerun_check.tsv.",
    ),
):
    """
    Check sessions: compare lab note reruns vs BIDS extra runs.

    Pass -s 06,10 for a single session or -f subseslist.tsv for batch.
    """
    console.print("\n[bold cyan]### Reading lab note...[/]")
    df_note = parse_lab_note(lab_note)
    console.print(
        f"  {len(df_note)} rerun row(s) across "
        f"{df_note[['sub', 'ses']].drop_duplicates().shape[0]} session(s)."
    )

    pairs = _resolve_pairs(subses, subses_file, df_note)
    if subses_file is not None or subses is None:
        _cross_check_subseslist_vs_labnote(pairs, df_note)

    table = Table(show_header=True, header_style="bold magenta", title="Rerun Check")
    table.add_column("sub")
    table.add_column("ses")
    table.add_column("note\nextra", justify="right")
    table.add_column("bids\nextra", justify="right")
    table.add_column("within\nrange", justify="right")
    table.add_column("status")

    report_rows = []
    for sub, ses in pairs:
        r = check_single_session(str(bidsdir), sub, ses, df_note)
        _print_session_result(r)

        colour = {
            "OK": "green",
            "WITHIN_RANGE_ONLY": "cyan",
            "MISMATCH": "bold red",
            "NO_FUNC_DIR": "yellow",
        }.get(r["status"], "white")
        table.add_row(
            r["sub"],
            r["ses"],
            str(len(r["note_set"])),
            str(len(r["bids_set"])),
            str(len(r["within_range"])),
            f"[{colour}]{r['status']}[/]",
        )

        for _, nr in r["note_rows"].iterrows():
            report_rows.append(
                {
                    "sub": r["sub"],
                    "ses": r["ses"],
                    "task": nr.task,
                    "extra_run": nr.extra_run,
                    "compensates_run": nr.compensates_run,
                    "protocol_name": nr.protocol_name,
                    "found_in_bids": (nr.task, nr.extra_run) in r["bids_set"],
                    "is_within_range": (nr.task, nr.extra_run) in r["within_range"],
                    "status": r["status"],
                }
            )

    console.print("\n")
    console.print(table)

    out_dir = (
        str(output_tsv.parent)
        if output_tsv
        else op.join(str(bidsdir), "sourcedata", "qc")
    )
    out_file = str(output_tsv) if output_tsv else op.join(out_dir, "rerun_check.tsv")
    os.makedirs(out_dir, exist_ok=True)
    fieldnames = [
        "sub",
        "ses",
        "task",
        "extra_run",
        "compensates_run",
        "protocol_name",
        "found_in_bids",
        "is_within_range",
        "status",
    ]
    with open(out_file, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames, delimiter="\t")
        w.writeheader()
        w.writerows(report_rows)
    console.print(f"[dim]Report → {out_file}[/]")


@app.command("fix")
def cmd_fix(
    bidsdir: Path = typer.Option(..., "--bidsdir"),
    subses: Optional[str] = typer.Option(
        None, "--subses", "-s", help="Single sub,ses pair e.g. 06,10"
    ),
    subses_file: Optional[Path] = typer.Option(
        None, "--file", "-f", help="Subseslist TSV/CSV with sub and ses columns."
    ),
    execute: bool = typer.Option(
        False, "--execute", help="Apply changes (default: dry-run)."
    ),
    offset_min: int = typer.Option(
        300,
        "--offset-min",
        help="Min expected mat−bids offset in seconds (default 300).",
    ),
    offset_max: int = typer.Option(
        550,
        "--offset-max",
        help="Max expected mat−bids offset in seconds (default 550).",
    ),
):
    """
    Fix BIDS task/run label mismatches by matching params.mat to BIDS bold files.

    Dry-run by default — pass --execute to rename files.
    Pass -s 06,10 for a single session or -f subseslist.tsv for batch.
    """
    dry_run = not execute

    if dry_run:
        console.print("\n[bold yellow]─── DRY-RUN ─── (no files changed)[/bold yellow]")
    else:
        console.print("\n[bold green]─── EXECUTE ─── (renaming files)[/bold green]")

    # Resolve pairs without lab note (not needed for rename-based fix)
    if subses_file is not None:
        pairs = _pairs_from_subses_list(subses_file)
    elif subses is not None:
        parts = [p.strip().zfill(2) for p in subses.split(",")]
        if len(parts) != 2:
            console.print("[red]--subses must be sub,ses e.g. 06,10[/]")
            raise typer.Exit(1)
        pairs = [(parts[0], parts[1])]
    else:
        console.print("[red]Provide -s sub,ses or -f subseslist.[/]")
        raise typer.Exit(1)

    total_renamed = 0
    for sub, ses in pairs:
        result = check_and_fix_session(
            str(bidsdir),
            sub,
            ses,
            dry_run=dry_run,
            offset_min=offset_min,
            offset_max=offset_max,
        )
        total_renamed += result.get("renamed", 0)

    if dry_run:
        console.print(
            "\n[dim]Pass [bold]--execute[/bold] to apply the renames above.[/dim]"
        )
    else:
        console.print(
            f"\n[bold green]Done:[/bold green] {total_renamed} file(s) renamed."
        )


@app.command("parse")
def cmd_parse(
    lab_note: Path = typer.Option(..., "--lab-note"),
    debug: bool = typer.Option(False, "--debug"),
    sub: Optional[str] = typer.Option(
        None, "--sub", help="Show raw rows for this subject only."
    ),
    ses: Optional[str] = typer.Option(
        None, "--ses", help="Show raw rows for this session only."
    ),
):
    """Parse the lab note and print all rerun rows.

    Use --sub / --ses together with --debug to inspect the raw unfiltered rows
    for a specific subject/session directly from the Excel sheet.
    """
    console.print(f"\n[bold cyan]### Parsing: {lab_note}[/]")

    # Raw-sheet debug: show every row for the requested sub before any filtering
    if debug and sub is not None:
        sub_zf = sub.zfill(2)
        sheet_name = f"sub-{sub_zf}"
        ext = lab_note.suffix.lower()
        if ext in (".xlsx", ".xls"):
            xls = pd.ExcelFile(lab_note)
            if sheet_name in xls.sheet_names:
                raw = pd.read_excel(xls, sheet_name=sheet_name, header=0)
                needed = [
                    c for c in ["sub", "ses", "protocol_name"] if c in raw.columns
                ]
                raw_sub = raw[needed].copy() if needed else raw.copy()
                # apply ffill + zfill so ses values are normalised, then filter by ses
                raw_sub[["sub", "ses"]] = (
                    raw_sub[["sub", "ses"]].replace("", pd.NA).ffill()
                )
                raw_sub["sub"] = raw_sub["sub"].apply(_safe_zfill)
                raw_sub["ses"] = raw_sub["ses"].apply(_safe_zfill)
                if ses is not None:
                    raw_sub = raw_sub[raw_sub["ses"] == ses.zfill(2)]
                console.print(
                    f"\n[bold yellow]--- RAW rows for {sheet_name}"
                    + (f" ses-{ses.zfill(2)}" if ses else "")
                    + " (before rerun filter) ---[/]"
                )
                console.print(raw_sub.to_string())
                console.print("[bold yellow]--- END RAW ---[/]\n")
            else:
                console.print(
                    f"[yellow]Sheet '{sheet_name}' not found in {lab_note}[/]"
                )
        else:
            console.print("[yellow]Raw-sheet debug only works for .xlsx files.[/]")

    df = parse_lab_note(lab_note)

    if debug:
        console.print(f"\n[bold yellow]shape: {df.shape}[/]")
        console.print(f"dtypes:\n{df.dtypes}")
        console.print(f"values:\n{df.to_string()}\n")

    if df.empty:
        console.print("[yellow]No rerun rows found.[/]")
        raise typer.Exit()

    console.print(
        f"[green]{len(df)} row(s) across "
        f"{df[['sub', 'ses']].drop_duplicates().shape[0]} session(s).[/]\n"
    )
    table = Table(show_header=True, header_style="bold magenta", title="Parsed reruns")
    for col in ["sub", "ses", "task", "extra_run", "compensates_run"]:
        table.add_column(col)
    table.add_column("protocol_name", style="dim")

    for _, row in df.iterrows():
        table.add_row(
            row["sub"],
            row["ses"],
            row["task"],
            row["extra_run"],
            row["compensates_run"],
            row["protocol_name"],
        )
    console.print(table)


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app()
