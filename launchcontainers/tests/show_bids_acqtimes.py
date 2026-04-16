"""
show_bids_acqtimes.py
---------------------
For each sub/ses, read AcquisitionTime from ALL modality JSON sidecars
(anat / func / dwi / fmap) and print a table sorted by time.

When --labnote / -l is supplied, compares BIDS against the lab-note
spreadsheet (VOTCLOC_subses_list.xlsx) and reports mismatches.

Output layers
-------------
default  — one-line summary per session + final cross-session table
-v       — adds per-session detailed comparison table (all rows)
-o DIR   — writes three files to DIR:
               summary.tsv          one row per session with counts
               error_subses.tsv     only sessions with issues (sub, ses, issue detail)
               detailed_log.txt     full verbose output for every session

Lab-note Excel columns expected
---------------------------------
    sub | ses | date | protocol_name | time_start | quality_mark | comment
    (sub/ses read as strings and zero-padded)

Protocol → BIDS mapping
------------------------
    T1_MP2RAGE   →  anat  *T1w*
    T2           →  anat  *T2w*
    fmap_01/02…  →  fmap  *run-{N}*epi*
    fLoc_run-NN  →  func  *task-fLoc_run-NN_bold*
    retRW_run-NN →  func  *task-retRW_run-NN_bold*   (etc.)
    (eyetracker / pause / calibration → SKIP, no BIDS counterpart)

Status per row
--------------
    MATCH    BIDS file found; time within 10 min of lab note
    WARN     BIDS file found; time differs > 10 min
    MISSING  lab note entry has no matching BIDS file
    EXTRA    BIDS file exists with no lab note entry
    SKIP     lab note entry with no BIDS counterpart

Usage
-----
    python show_bids_acqtimes.py --bidsdir /path/BIDS -s 07,03
    python show_bids_acqtimes.py --bidsdir /path/BIDS -s 07,03 --task "retfix*"
    python show_bids_acqtimes.py --bidsdir /path/BIDS -f subseslist.tsv \\
        --labnote /path/VOTCLOC_subses_list.xlsx
    python show_bids_acqtimes.py --bidsdir /path/BIDS -f subseslist.tsv \\
        --labnote /path/VOTCLOC_subses_list.xlsx -v -o ./output
"""

from __future__ import annotations

import csv
import fnmatch
import glob
import json
import os
import os.path as op
import re
from datetime import datetime, time as dtime
from io import StringIO
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from launchcontainers.utils import parse_subses_list

console = Console()
app = typer.Typer(pretty_exceptions_show_locals=False)

MODALITIES = ("anat", "func", "dwi", "fmap")
TIME_WARN_SEC = 600  # 10-minute tolerance for lab-note vs BIDS time comparison
TIME_ERROR_SEC = (
    180  # 3-minute threshold → status becomes WARN (flagged as error in summary)
)


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def _to_sec(t: str | dtime | None) -> float:
    """Convert HH:MM:SS[.xxx] string or datetime.time to seconds since midnight."""
    if t is None or t == "":
        return float("inf")
    if isinstance(t, dtime):
        return t.hour * 3600 + t.minute * 60 + t.second + t.microsecond / 1e6
    try:
        parts = str(t).split(":")
        return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
    except Exception:
        return float("inf")


def _fmt_time(t: str | dtime | None) -> str:
    """Return a HH:MM:SS display string."""
    if t is None or t == "":
        return ""
    if isinstance(t, dtime):
        return t.strftime("%H:%M:%S")
    return str(t).split(".")[0]


def _short_name(basename: str, sub: str, ses: str) -> str:
    name = re.sub(rf"^sub-{sub}_ses-{ses}_", "", basename)
    return re.sub(r"\.json$", "", name)


# ---------------------------------------------------------------------------
# BIDS collection
# ---------------------------------------------------------------------------


def _find_ses_dirs(bidsdir: str, sub: str, ses: str) -> list[str]:
    """
    Return all session directories matching ses-{ses}* sorted by name.
    e.g. ses-02 and ses-02part2 both match ses=02.
    """
    sub_dir = op.join(bidsdir, f"sub-{sub}")
    pattern = op.join(sub_dir, f"ses-{ses}*")
    matches = sorted(glob.glob(pattern))
    return [d for d in matches if op.isdir(d)]


def _collect_session(bidsdir: str, sub: str, ses: str) -> list[dict]:
    """
    Return one dict per JSON sidecar across all modalities, sorted by AcquisitionTime.
    Automatically merges split sessions (ses-{ses}, ses-{ses}part2, …) into one list.
    Keys: modality, name, acq_time (str), acq_sec (float), matched (bool), ses_part (str).
    """
    ses_dirs = _find_ses_dirs(bidsdir, sub, ses)
    if not ses_dirs:
        return []

    rows = []
    for ses_dir in ses_dirs:
        # Derive the actual ses label from the directory name (e.g. "02part2")
        ses_label = op.basename(ses_dir).replace("ses-", "")

        for mod in MODALITIES:
            mod_dir = op.join(ses_dir, mod)
            if not op.isdir(mod_dir):
                continue
            for jf in sorted(
                glob.glob(op.join(mod_dir, f"sub-{sub}_ses-{ses_label}_*.json"))
            ):
                acq_time = ""
                try:
                    with open(jf) as fh:
                        acq_time = json.load(fh).get("AcquisitionTime", "")
                except Exception:
                    pass
                rows.append(
                    {
                        "modality": mod,
                        "name": _short_name(op.basename(jf), sub, ses_label),
                        "acq_time": acq_time,
                        "acq_sec": _to_sec(acq_time),
                        "matched": False,
                        "ses_part": ses_label,  # "02" or "02part2" etc.
                    }
                )

    rows.sort(key=lambda r: r["acq_sec"])

    # --- DWI and fmap deduplication ---
    # fmap : dir-AP and dir-PA are paired; only AP is compared against the lab note.
    # DWI  : acq-magonly, acq-nordic, dir-AP, dir-PA for the same run-XX are all
    #         the same logical acquisition — keep one representative per run.
    dwi_seen_runs: set[str] = set()
    deduped: list[dict] = []
    for r in rows:
        if r["modality"] == "fmap":
            if "_dir-PA_" in r["name"]:
                continue  # PA is the phase-encode companion; skip
            deduped.append(r)
        elif r["modality"] == "dwi":
            # Match _dwi (BIDS) or _magnitude (raw_nifti) suffix
            run_m = re.search(r"_run-(\w+)_(dwi|magnitude)$", r["name"])
            run_key = run_m.group(1) if run_m else r["name"]
            if run_key not in dwi_seen_runs:
                dwi_seen_runs.add(run_key)
                deduped.append(r)
            # else: silently drop — run already represented by first row
        else:
            deduped.append(r)

    return deduped


# ---------------------------------------------------------------------------
# Lab-note loading
# ---------------------------------------------------------------------------


def _safe_zfill(v) -> str:
    s = str(v).strip()
    try:
        return str(int(float(s))).zfill(2)
    except (ValueError, TypeError):
        return s


def _clean_subses_df(df) -> object:
    """Forward-fill sub/ses (merged cells appear as NaN after row 1), then zfill."""
    import pandas as pd

    df = df.copy()
    df[["sub", "ses"]] = df[["sub", "ses"]].replace("", pd.NA).ffill()
    df = df.dropna(subset=["sub", "ses"])
    df["sub"] = df["sub"].apply(_safe_zfill)
    df["ses"] = df["ses"].apply(_safe_zfill)
    return df


def _load_labnote_df(xlsx_path: str):
    """
    Read all 'sub-*' sheets, concatenate, return a single cleaned DataFrame.
    Columns: sub, ses, protocol_name, time_start, quality_mark, (others ignored).
    """
    try:
        import pandas as pd
    except ImportError:
        console.print("[red]pandas not installed – cannot read lab note.[/]")
        raise typer.Exit(1)

    xls = pd.ExcelFile(xlsx_path)
    sheets = [s for s in xls.sheet_names if s.startswith("sub-")]
    if not sheets:
        console.print(f"[yellow]No 'sub-*' sheets found in {xlsx_path}[/]")
        return None

    parts = []
    for sheet in sheets:
        df_sheet = pd.read_excel(xls, sheet_name=sheet, header=0)
        df_sheet.columns = [
            c.strip().lower().replace(" ", "_") for c in df_sheet.columns
        ]
        if not {"sub", "ses", "protocol_name"}.issubset(df_sheet.columns):
            continue
        df_sheet = _clean_subses_df(df_sheet)
        parts.append(df_sheet)

    if not parts:
        console.print(f"[yellow]No valid sheets found in {xlsx_path}[/]")
        return None

    return pd.concat(parts, ignore_index=True)


def _normalize_labnote_rows(rows: list[dict]) -> list[dict]:
    """
    Fix two known lab-note quirks before comparison:

    1. Strip ``_rerun-XX`` suffix from any protocol name.
       e.g. ``fLoc_run-11_rerun-01`` → ``fLoc_run-11``

    2. Renumber duplicate or mis-numbered run IDs sequentially.
       Applies per protocol family (fmap, fLoc, retRW, …).
       e.g. two ``fmap_02`` entries → ``fmap_01``, ``fmap_02``
       e.g. ``fLoc_run-11`` after stripping rerun → renumbered in position order
    """
    # Step 1 — strip _rerun-XX
    for r in rows:
        r["protocol_name"] = re.sub(
            r"_rerun-\d+$", "", r["protocol_name"], flags=re.IGNORECASE
        )

    # Step 2 — renumber per family
    # Identify family of each row:
    #   "fmap"       for fmap_NN
    #   "task_XXXX"  for XXXX_run-NN  (e.g. fLoc, retRW …)
    #   None         for everything else (no renumbering)
    family_indices: dict[str, list[int]] = {}  # family → [row indices]
    for i, r in enumerate(rows):
        p = r["protocol_name"].strip()
        if re.match(r"fmap_\d+$", p, re.IGNORECASE):
            family_indices.setdefault("fmap", []).append(i)
        else:
            m = re.match(r"([A-Za-z][A-Za-z0-9]*)_run-\d+$", p)
            if m:
                family_indices.setdefault(f"task_{m.group(1)}", []).append(i)

    # Reassign run numbers 1, 2, 3… within each family (preserving order)
    for family, indices in family_indices.items():
        for new_num, idx in enumerate(indices, start=1):
            p = rows[idx]["protocol_name"]
            if family == "fmap":
                rows[idx]["protocol_name"] = f"fmap_{new_num:02d}"
            else:
                task = re.match(r"([A-Za-z][A-Za-z0-9]*)_run-\d+$", p).group(1)
                rows[idx]["protocol_name"] = f"{task}_run-{new_num:02d}"

    return rows


def _labnote_rows_for(df, sub: str, ses: str) -> list[dict]:
    """Filter the big labnote DataFrame for one sub/ses and return row dicts."""
    subset = df[(df["sub"] == sub) & (df["ses"] == ses)]
    rows = []
    for _, row in subset.iterrows():
        proto = str(row.get("protocol_name", "")).strip()
        if not proto or proto.lower() == "nan":
            continue
        ts = row.get("time_start", None)
        qual = str(row.get("quality_mark", "")).strip()
        rows.append(
            {
                "protocol_name": proto,
                "time_str": _fmt_time(ts),
                "time_sec": _to_sec(ts),
                "quality": qual if qual and qual.lower() != "nan" else "",
            }
        )
    return _normalize_labnote_rows(rows)


# ---------------------------------------------------------------------------
# Protocol → BIDS matching
# ---------------------------------------------------------------------------

_SKIP_RE = re.compile(
    r"^(eye|eyetracker|pause|MRS|localizer|scout|cali|vali)",
    re.IGNORECASE,
)

# Lab-note task name → list of possible BIDS task labels (tried in order).
# Add new aliases here when naming conventions diverge between the lab note
# and the BIDS filenames.
TASK_ALIASES: dict[str, list[str]] = {
    "retRW": ["retRW", "retfixRW"],
    "retFF": ["retFF", "retfixFF"],
    "retCB": [
        "retCB",
        "retfixCB",
        "retfixRWblock",
        "retfixRWblock01",
        "retfixRWblock02",
    ],
}


def _parse_protocol(protocol_name: str) -> dict | None:
    """
    Map a lab-note protocol_name to a BIDS search dict, or None (→ SKIP).
    Returned dict keys:
        modality  – anat / func / dwi / fmap
        patterns  – list of substring patterns; a BIDS entry matches if ANY
                    pattern is found in its short name
    """
    p = protocol_name.strip()
    if _SKIP_RE.match(p):
        return None
    if re.match(r"T1", p, re.IGNORECASE):
        # raw_nifti stores the T1 UNI image (not T1w); match both
        return {"modality": "anat", "patterns": ["T1w", "_uni"]}
    if re.match(r"T2$", p, re.IGNORECASE):
        return {"modality": "anat", "patterns": ["T2w"]}
    m = re.match(r"fmap_(\d+)$", p, re.IGNORECASE)
    if m:
        run_num = str(int(m.group(1)))
        return {"modality": "fmap", "patterns": [f"run-{run_num}_epi"]}
    # DWI — lab note may say "DWI" or "DWI_run-01" / "DWI_run-1"
    # raw_nifti stores DWI as _magnitude instead of _dwi
    m = re.match(r"DWI(?:_run-(\d+))?$", p, re.IGNORECASE)
    if m:
        if m.group(1):
            run_raw = m.group(1)
            run_pad = run_raw.zfill(2)
            run_unpad = str(int(run_raw))
            # Match either zero-padded or un-padded run; _dwi (BIDS) or _magnitude (raw)
            patterns = list(
                {
                    f"run-{run_pad}_dwi",
                    f"run-{run_unpad}_dwi",
                    f"run-{run_pad}_magnitude",
                    f"run-{run_unpad}_magnitude",
                }
            )
        else:
            patterns = ["_dwi", "_magnitude"]  # any DWI run (BIDS or raw)
        return {"modality": "dwi", "patterns": patterns}

    # func: task_run-NN  (e.g. fLoc_run-01, retRW_run-02)
    m = re.match(r"([A-Za-z][A-Za-z0-9]*)_run-(\d+)$", p)
    if m:
        task = m.group(1)
        run = m.group(2).zfill(2)
        # Expand aliases: each alias → its own bold/magnitude pattern
        # raw_nifti sessions store func as _magnitude instead of _bold
        task_labels = TASK_ALIASES.get(task, [task])
        patterns = [
            f"task-{tl}_run-{run}_{suf}"
            for tl in task_labels
            for suf in ("bold", "magnitude")
        ]
        return {"modality": "func", "patterns": patterns}
    return None


def _find_bids_rows(bids_rows: list[dict], proto_info: dict) -> list[dict]:
    """Return BIDS rows matching the protocol modality and ANY of its patterns."""
    mod = proto_info["modality"]
    patterns = proto_info["patterns"]
    return [
        r
        for r in bids_rows
        if r["modality"] == mod and any(pat in r["name"] for pat in patterns)
    ]


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def _compare_session(bids_rows: list[dict], labnote_rows: list[dict]) -> list[dict]:
    """
    Align lab-note entries with BIDS rows.
    Returns comparison dicts; each includes proto_mod for modality attribution.
    """
    bids = [dict(r) for r in bids_rows]
    result = []

    for ln in labnote_rows:
        proto_info = _parse_protocol(ln["protocol_name"])

        if proto_info is None:
            result.append(
                {
                    "lab_time": ln["time_str"],
                    "protocol": ln["protocol_name"],
                    "quality": ln["quality"],
                    "bids_time": "",
                    "bids_name": "",
                    "bids_mod": "",
                    "bids_ses_part": "",
                    "proto_mod": "",
                    "delta_min": None,
                    "status": "SKIP",
                }
            )
            continue

        proto_mod = proto_info["modality"]
        matches = _find_bids_rows(bids, proto_info)

        if not matches:
            result.append(
                {
                    "lab_time": ln["time_str"],
                    "protocol": ln["protocol_name"],
                    "quality": ln["quality"],
                    "bids_time": "",
                    "bids_name": "",
                    "bids_mod": "",
                    "bids_ses_part": "",
                    "proto_mod": proto_mod,
                    "delta_min": None,
                    "status": "MISSING",
                }
            )
            continue

        for match in matches:
            match["matched"] = True
            lab_sec = ln["time_sec"]
            bids_sec = match["acq_sec"]
            delta = (
                abs(bids_sec - lab_sec)
                if lab_sec != float("inf") and bids_sec != float("inf")
                else None
            )
            if delta is None or delta <= TIME_ERROR_SEC:
                status = "MATCH"
            else:
                status = "WARN"  # >3 min difference → flagged
            result.append(
                {
                    "lab_time": ln["time_str"],
                    "protocol": ln["protocol_name"],
                    "quality": ln["quality"],
                    "bids_time": match["acq_time"],
                    "bids_name": match["name"],
                    "bids_mod": match["modality"],
                    "bids_ses_part": match.get("ses_part", ""),
                    "proto_mod": proto_mod,
                    "delta_min": round(delta / 60, 1) if delta is not None else None,
                    "status": status,
                }
            )

    # EXTRA: unmatched BIDS files — skip func files that are not bold or magnitude
    # (sbref, phase, gfactor are not recorded in the lab note;
    #  magnitude is used instead of bold in raw_nifti sessions)
    for r in bids:
        if not r["matched"]:
            if r["modality"] == "func" and not (
                r["name"].endswith("_bold") or r["name"].endswith("_magnitude")
            ):
                continue
            # fLoc runs > 10 are expected overflow runs; don't flag as EXTRA
            if r["modality"] == "func":
                m = re.search(r"task-fLoc_run-(\d+)_(bold|magnitude)", r["name"])
                if m and int(m.group(1)) > 10:
                    continue
            result.append(
                {
                    "lab_time": "",
                    "protocol": "",
                    "quality": "",
                    "bids_time": r["acq_time"],
                    "bids_name": r["name"],
                    "bids_mod": r["modality"],
                    "bids_ses_part": r.get("ses_part", ""),
                    "proto_mod": r["modality"],
                    "delta_min": None,
                    "status": "EXTRA",
                }
            )

    return result


# ---------------------------------------------------------------------------
# Session result aggregation
# ---------------------------------------------------------------------------


def _modality_counts(cmp_rows: list[dict]) -> dict[str, dict[str, int]]:
    """
    Return per-modality status counts, e.g.:
        {"anat": {"MATCH": 2, "MISSING": 1}, "func": {"MATCH": 12}, ...}
    Skips SKIP rows.
    """
    counts: dict[str, dict[str, int]] = {m: {} for m in MODALITIES}
    for r in cmp_rows:
        if r["status"] == "SKIP":
            continue
        mod = r["proto_mod"] or r["bids_mod"]
        if not mod or mod not in counts:
            continue
        counts[mod][r["status"]] = counts[mod].get(r["status"], 0) + 1
    return counts


def _session_status(counts: dict[str, dict[str, int]]) -> str:
    """Return overall session status: OK / WARN / ISSUES."""
    all_counts = {}
    for mc in counts.values():
        for k, v in mc.items():
            all_counts[k] = all_counts.get(k, 0) + v
    if all_counts.get("MISSING", 0) or all_counts.get("EXTRA", 0):
        return "ISSUES"
    if all_counts.get("WARN", 0):
        return "WARN"
    return "OK"


def _modality_cell(mc: dict[str, int]) -> str:
    """Format one modality column cell for the summary table."""
    if not mc:
        return "—"
    if set(mc.keys()) <= {"MATCH"}:
        return f"[green]✓ {mc['MATCH']}[/]"
    parts = []
    if mc.get("MATCH"):
        parts.append(f"[green]{mc['MATCH']}ok[/]")
    if mc.get("MISSING"):
        parts.append(f"[red]M:{mc['MISSING']}[/]")
    if mc.get("WARN"):
        parts.append(f"[yellow]W:{mc['WARN']}[/]")
    if mc.get("EXTRA"):
        parts.append(f"[magenta]+{mc['EXTRA']}[/]")
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

_STATUS_COLOR = {
    "MATCH": "green",
    "WARN": "yellow",
    "MISSING": "red",
    "EXTRA": "magenta",
    "SKIP": "dim",
}


def _print_bids_table(sub: str, ses: str, rows: list[dict]) -> None:
    """Plain BIDS acq-time table (no lab note)."""
    parts = sorted({r.get("ses_part", ses) for r in rows})
    split_note = (
        f"  [dim]({' + '.join(f'ses-{p}' for p in parts)})[/]" if len(parts) > 1 else ""
    )
    console.print(
        f"\n[bold cyan]sub-{sub}  ses-{ses}[/]{split_note}  ({len(rows)} JSON sidecars)"
    )
    if not rows:
        console.print("  [yellow]No JSON sidecars found.[/]")
        return
    t = Table(show_header=True, header_style="bold magenta", box=None, padding=(0, 1))
    t.add_column("acq_time", justify="right", style="cyan")
    t.add_column("ses_part", justify="center", style="dim")
    t.add_column("modality", justify="center", style="dim")
    t.add_column("name")
    for r in rows:
        part = r.get("ses_part", ses)
        part_style = "yellow" if part != ses else "dim"
        t.add_row(
            r["acq_time"] or "[dim]—[/]",
            f"[{part_style}]{part}[/]",
            r["modality"],
            r["name"],
        )
    console.print(t)


def _print_session_detail(sub: str, ses: str, cmp_rows: list[dict]) -> None:
    """Verbose: per-session side-by-side comparison table."""
    n_miss = sum(1 for r in cmp_rows if r["status"] == "MISSING")
    n_warn = sum(1 for r in cmp_rows if r["status"] == "WARN")
    n_extra = sum(1 for r in cmp_rows if r["status"] == "EXTRA")
    flag = (
        " [red]✗[/]"
        if (n_miss or n_extra)
        else (" [yellow]⚠[/]" if n_warn else " [green]✓[/]")
    )

    console.print(
        f"\n[bold cyan]sub-{sub}  ses-{ses}[/]{flag}  "
        f"[red]MISSING {n_miss}[/]  [yellow]WARN {n_warn}[/]  [magenta]EXTRA {n_extra}[/]"
    )

    t = Table(show_header=True, header_style="bold magenta", box=None, padding=(0, 1))
    t.add_column("lab_time", justify="right", style="dim")
    t.add_column("protocol", style="dim")
    t.add_column("quality", justify="center", style="dim")
    t.add_column("bids_time", justify="right", style="cyan")
    t.add_column("ses_part", justify="center", style="dim")
    t.add_column("bids_entry")
    t.add_column("Δmin", justify="right")
    t.add_column("status", justify="center")

    for r in cmp_rows:
        st = r["status"]
        color = _STATUS_COLOR.get(st, "white")
        delta = str(r["delta_min"]) if r["delta_min"] is not None else ""
        ses_part = r.get("bids_ses_part", "")
        t.add_row(
            r["lab_time"],
            r["protocol"],
            r["quality"],
            r["bids_time"][:8] if r["bids_time"] else "",
            f"[yellow]{ses_part}[/]" if ses_part and ses_part != ses else ses_part,
            f"[{color}]{r['bids_name']}[/]" if r["bids_name"] else "",
            delta,
            f"[{color}]{st}[/]",
        )
    console.print(t)


def _print_summary_table(results: list[dict]) -> None:
    """Streamline: one row per session with per-modality status columns."""
    n_ok = sum(1 for r in results if r["status"] == "OK")
    n_warn = sum(1 for r in results if r["status"] == "WARN")
    n_issues = sum(1 for r in results if r["status"] == "ISSUES")
    n_err = sum(1 for r in results if r["status"] == "ERROR")

    t = Table(
        title="BIDS vs lab-note summary",
        show_header=True,
        header_style="bold magenta",
        box=None,
        padding=(0, 1),
    )
    t.add_column("sub", justify="right", style="cyan")
    t.add_column("ses", justify="right", style="cyan")
    t.add_column("anat", justify="center")
    t.add_column("func", justify="center")
    t.add_column("dwi", justify="center")
    t.add_column("fmap", justify="center")
    t.add_column("status", justify="center")

    for r in results:
        st = r["status"]
        if st == "OK":
            st_str = "[green]OK[/]"
        elif st == "WARN":
            st_str = "[yellow]WARN[/]"
        elif st == "ISSUES":
            st_str = "[red]ISSUES[/]"
        elif st == "NO_LABNOTE":
            st_str = "[dim]no labnote[/]"
        else:
            st_str = f"[red]{st}[/]"

        counts = r.get("counts", {})
        t.add_row(
            r["sub"],
            r["ses"],
            _modality_cell(counts.get("anat", {})),
            _modality_cell(counts.get("func", {})),
            _modality_cell(counts.get("dwi", {})),
            _modality_cell(counts.get("fmap", {})),
            st_str,
        )

    console.print(t)
    console.print(
        f"\n[green]{n_ok} OK[/]  [yellow]{n_warn} WARN[/]  "
        f"[red]{n_issues} ISSUES[/]"
        + (f"  [red]{n_err} ERROR[/]" if n_err else "")
        + f"  out of {len(results)} sessions"
    )


# ---------------------------------------------------------------------------
# Output file writing
# ---------------------------------------------------------------------------


def _write_outputs(results: list[dict], outdir: str) -> None:
    """Write summary.tsv, error_subses.tsv, and detailed_log.txt to outdir."""
    os.makedirs(outdir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1. summary.tsv
    summary_path = op.join(outdir, "summary.tsv")
    with open(summary_path, "w", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(
            [
                "sub",
                "ses",
                "status",
                "anat_match",
                "anat_missing",
                "anat_warn",
                "anat_extra",
                "func_match",
                "func_missing",
                "func_warn",
                "func_extra",
                "dwi_match",
                "dwi_missing",
                "dwi_warn",
                "dwi_extra",
                "fmap_match",
                "fmap_missing",
                "fmap_warn",
                "fmap_extra",
            ]
        )
        for r in results:
            c = r.get("counts", {})
            row = [r["sub"], r["ses"], r["status"]]
            for mod in MODALITIES:
                mc = c.get(mod, {})
                row += [
                    mc.get("MATCH", 0),
                    mc.get("MISSING", 0),
                    mc.get("WARN", 0),
                    mc.get("EXTRA", 0),
                ]
            writer.writerow(row)

    # 2. error_subses.tsv  (sessions with ISSUES or WARN only)
    error_path = op.join(outdir, "error_subses.tsv")
    with open(error_path, "w", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(
            ["sub", "ses", "status", "modality", "row_status", "protocol", "bids_entry"]
        )
        for r in results:
            if r["status"] in ("OK", "NO_LABNOTE", "ERROR"):
                continue
            for row in r.get("cmp_rows", []):
                if row["status"] in ("MATCH", "SKIP"):
                    continue
                writer.writerow(
                    [
                        r["sub"],
                        r["ses"],
                        r["status"],
                        row["proto_mod"] or row["bids_mod"],
                        row["status"],
                        row["protocol"],
                        row["bids_name"],
                    ]
                )

    # 3. detailed_log.txt  (full verbose output captured as plain text)
    log_path = op.join(outdir, f"detailed_log_{timestamp}.txt")
    capture = Console(file=StringIO(), highlight=False, markup=True)
    for r in results:
        if r.get("cmp_rows"):
            # Re-render using capture console
            _render_session_detail_to(capture, r["sub"], r["ses"], r["cmp_rows"])
        elif r.get("bids_rows"):
            capture.print(f"\nsub-{r['sub']}  ses-{r['ses']}  (BIDS only, no labnote)")
            for b in r["bids_rows"]:
                capture.print(f"  {b['acq_time']:>15}  {b['modality']:6}  {b['name']}")
    with open(log_path, "w") as fh:
        fh.write(capture.file.getvalue())

    console.print(f"\n[dim]Output written to {outdir}/[/]")
    console.print(f"  summary     → {op.basename(summary_path)}")
    console.print(f"  errors      → {op.basename(error_path)}")
    console.print(f"  detail log  → {op.basename(log_path)}")


def _render_session_detail_to(
    cap: Console, sub: str, ses: str, cmp_rows: list[dict]
) -> None:
    """Write the per-session detail to a capture Console (for log file)."""
    n_miss = sum(1 for r in cmp_rows if r["status"] == "MISSING")
    n_warn = sum(1 for r in cmp_rows if r["status"] == "WARN")
    n_extra = sum(1 for r in cmp_rows if r["status"] == "EXTRA")
    cap.print(
        f"\nsub-{sub}  ses-{ses}  MISSING:{n_miss}  WARN:{n_warn}  EXTRA:{n_extra}"
    )
    cap.print(
        f"  {'lab_time':>8}  {'protocol':<25}  {'Q':3}  {'bids_time':>8}  {'bids_entry':<40}  {'Δmin':>5}  status"
    )
    for r in cmp_rows:
        delta = f"{r['delta_min']:5.1f}" if r["delta_min"] is not None else "     "
        cap.print(
            f"  {r['lab_time']:>8}  {r['protocol']:<25}  {r['quality']:<3}  "
            f"{r['bids_time'][:8] if r['bids_time'] else '':>8}  "
            f"{r['bids_name']:<40}  {delta}  {r['status']}"
        )


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
        help="Single sub,ses pair e.g. 07,03",
    ),
    subses_file: Optional[Path] = typer.Option(
        None,
        "--file",
        "-f",
        help="Subseslist file (TSV/CSV with sub,ses columns).",
    ),
    labnote: Optional[Path] = typer.Option(
        None,
        "--labnote",
        "-l",
        help="Path to lab-note Excel (e.g. VOTCLOC_subses_list.xlsx).",
    ),
    task: Optional[list[str]] = typer.Option(
        None,
        "--task",
        "-t",
        help="Filter to func files matching task pattern(s) (supports wildcards e.g. 'retfix*'). "
        "Repeat for multiple. Implies func-only view. Default: all modalities.",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show per-session detailed comparison table."
    ),
    outdir: Optional[Path] = typer.Option(
        None,
        "--outdir",
        "-o",
        help="Write summary.tsv, error_subses.tsv, and detailed_log.txt here.",
    ),
) -> None:
    """
    Show BIDS acquisition times.  With --labnote, compare against the lab-note
    spreadsheet; streamline summary by default, full detail with -v.
    Use --task to filter to specific func task(s) only (supports wildcards).
    """
    # --- Resolve session pairs ---
    if subses_file is not None:
        pairs = parse_subses_list(subses_file)
    elif subses is not None:
        parts = [p.strip().zfill(2) for p in subses.split(",")]
        if len(parts) != 2:
            console.print("[red]--subses must be sub,ses e.g. 07,03[/]")
            raise typer.Exit(1)
        pairs = [(parts[0], parts[1])]
    else:
        console.print("[red]Provide --subses/-s or --file/-f.[/]")
        raise typer.Exit(1)

    # --- Preload lab note (once) ---
    ln_df = None
    if labnote is not None:
        ln_df = _load_labnote_df(str(labnote))

    # --- Process each session ---
    results: list[dict] = []

    task_patterns = list(task) if task else []

    for sub, ses in pairs:
        bids_rows = _collect_session(str(bidsdir), sub, ses)

        # --task filter: keep only func rows whose task label matches a pattern
        if task_patterns:

            def _task_label(name: str) -> str:
                m = re.search(r"task-(\w+)_", name)
                return m.group(1) if m else ""

            bids_rows = [
                r
                for r in bids_rows
                if r["modality"] == "func"
                and any(
                    fnmatch.fnmatch(_task_label(r["name"]), p) for p in task_patterns
                )
            ]

        # No lab note → plain BIDS table mode
        if ln_df is None:
            if verbose:
                _print_bids_table(sub, ses, bids_rows)
            results.append(
                {
                    "sub": sub,
                    "ses": ses,
                    "status": "NO_LABNOTE",
                    "counts": {},
                    "cmp_rows": [],
                    "bids_rows": bids_rows,
                }
            )
            continue

        # Lab note available
        ln_rows = _labnote_rows_for(ln_df, sub, ses)
        if not ln_rows:
            console.print(f"  [yellow]sub-{sub} ses-{ses}: not found in lab note[/]")
            results.append(
                {
                    "sub": sub,
                    "ses": ses,
                    "status": "NO_LABNOTE",
                    "counts": {},
                    "cmp_rows": [],
                    "bids_rows": bids_rows,
                }
            )
            continue

        cmp_rows = _compare_session(bids_rows, ln_rows)
        counts = _modality_counts(cmp_rows)
        status = _session_status(counts)

        results.append(
            {
                "sub": sub,
                "ses": ses,
                "status": status,
                "counts": counts,
                "cmp_rows": cmp_rows,
                "bids_rows": bids_rows,
            }
        )

        # Verbose: print detail immediately per session
        if verbose:
            _print_session_detail(sub, ses, cmp_rows)

    # --- Streamline summary (always printed when labnote is given) ---
    if ln_df is not None:
        console.print()
        _print_summary_table(results)
    elif not verbose:
        # No labnote, no verbose → just print the BIDS tables now
        for r in results:
            _print_bids_table(r["sub"], r["ses"], r["bids_rows"])

    # --- Output files ---
    if outdir is not None:
        _write_outputs(results, str(outdir))


if __name__ == "__main__":
    app()
