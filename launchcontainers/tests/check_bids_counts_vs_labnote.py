"""
check_bids_counts_vs_labnote.py
--------------------------------
Compare BIDS acquisition counts and anchor times against the lab-note Excel
WITHOUT relying on the per-row timing in the lab note.

Strategy
--------
Instead of matching individual lab-note rows to BIDS files (unreliable due to
operator timing entries being approximate or missing), this script:

1. Reads the lab note → counts how many runs per category the session should have
   (T1w, T2w, fmap, fLoc, retRW, retFF, retCB, dwi).

2. Extracts 4 "anchor" times from the lab note (first recorded time of each):
       T1w  |  fmap  |  fLoc  |  ret (any ret* task)

3. Scans the BIDS session directory → counts runs per category and derives the
   same 4 anchor times from the JSON AcquisitionTime fields.

4. Flags:
     COUNT_MISMATCH  – BIDS run count ≠ lab-note count for a category
     TIME_WARN       – |BIDS anchor time − lab-note anchor time| > 3 min
     OK              – everything matches

Output layers
-------------
default  — one-liner per session + final cross-session summary table
-v       — adds per-session count table + anchor-time table
-o DIR   — writes summary.tsv, error_subses.tsv, detailed_log_*.txt

Normalisation (applied to lab-note protocol names before counting)
-------------------------------------------------------------------
* Strip ``_rerun-XX`` suffix
* Renumber duplicate / mis-numbered run IDs sequentially per family

Task aliases  (lab-note name → accepted BIDS task labels)
----------------------------------------------------------
    retRW → retRW, retfixRW
    retFF → retFF, retfixFF
    retCB → retCB, retfixCB, retfixRWblock, retfixRWblock01, retfixRWblock02

Usage
-----
    python check_bids_counts_vs_labnote.py --bidsdir /path/BIDS -s 07,03 \\
        --labnote /path/VOTCLOC_subses_list.xlsx
    python check_bids_counts_vs_labnote.py --bidsdir /path/BIDS -f subseslist.tsv \\
        --labnote /path/VOTCLOC_subses_list.xlsx -v -o ./output
"""

from __future__ import annotations

import csv
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
TIME_ERROR_SEC = 180  # > 3 min → TIME_WARN

# Categories tracked for count comparison (lab-note families)
# func tasks (fLoc, retRW, retFF, retCB) are NOT counted — only anchor times
# are checked for func. Counts are only compared for anat, fmap, and dwi.
COUNT_CATS = ["T1w", "T2w", "fmap", "dwi"]

# 4 anchor categories (first-time check)
ANCHOR_CATS = ["T1w", "fmap", "fLoc", "ret"]

# Lab-note task → list of valid BIDS task labels
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
# Reverse map: BIDS task label → lab-note category key
_BIDS_TO_LABNOTE_CAT: dict[str, str] = {}
for _ln_key, _bids_labels in TASK_ALIASES.items():
    for _bl in _bids_labels:
        _BIDS_TO_LABNOTE_CAT[_bl] = _ln_key
# Tasks without aliases map to themselves
for _cat in COUNT_CATS:
    if _cat not in _BIDS_TO_LABNOTE_CAT:
        _BIDS_TO_LABNOTE_CAT[_cat] = _cat

_SKIP_RE = re.compile(
    r"^(eye|eyetracker|pause|MRS|localizer|scout|cali|vali)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Generic helpers (shared with show_bids_acqtimes.py)
# ---------------------------------------------------------------------------


def _to_sec(t: str | dtime | None) -> float:
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
    if t is None or t == "":
        return ""
    if isinstance(t, dtime):
        return t.strftime("%H:%M:%S")
    return str(t).split(".")[0]


def _short_name(basename: str, sub: str, ses: str) -> str:
    name = re.sub(rf"^sub-{sub}_ses-{ses}_", "", basename)
    return re.sub(r"\.json$", "", name)


# ---------------------------------------------------------------------------
# BIDS collection (same dedup logic as show_bids_acqtimes.py)
# ---------------------------------------------------------------------------


def _collect_session(bidsdir: str, sub: str, ses: str) -> list[dict]:
    rows = []
    ses_dir = op.join(bidsdir, f"sub-{sub}", f"ses-{ses}")

    for mod in MODALITIES:
        mod_dir = op.join(ses_dir, mod)
        if not op.isdir(mod_dir):
            continue
        for jf in sorted(glob.glob(op.join(mod_dir, f"sub-{sub}_ses-{ses}_*.json"))):
            acq_time = ""
            try:
                with open(jf) as fh:
                    acq_time = json.load(fh).get("AcquisitionTime", "")
            except Exception:
                pass
            rows.append(
                {
                    "modality": mod,
                    "name": _short_name(op.basename(jf), sub, ses),
                    "acq_time": acq_time,
                    "acq_sec": _to_sec(acq_time),
                }
            )

    rows.sort(key=lambda r: r["acq_sec"])

    # fmap: drop dir-PA (keep only AP for comparison)
    # dwi:  keep one representative per run (first acq_time)
    dwi_seen: set[str] = set()
    deduped: list[dict] = []
    for r in rows:
        if r["modality"] == "fmap":
            if "_dir-PA_" in r["name"]:
                continue
            deduped.append(r)
        elif r["modality"] == "dwi":
            run_m = re.search(r"_run-(\w+)_dwi$", r["name"])
            key = run_m.group(1) if run_m else r["name"]
            if key not in dwi_seen:
                dwi_seen.add(key)
                deduped.append(r)
        else:
            deduped.append(r)

    return deduped


# ---------------------------------------------------------------------------
# BIDS → category
# ---------------------------------------------------------------------------


def _bids_row_category(r: dict) -> str | None:
    """
    Map one BIDS row to its count category key (e.g. "fLoc", "retRW", "T1w").
    Returns None for rows that don't count (sbref, phase, magnitude, gfactor, T2w-json, etc.).
    """
    mod = r["modality"]
    name = r["name"]

    if mod == "anat":
        if "T1w" in name:
            return "T1w"
        if "T2w" in name:
            return "T2w"
        return None

    if mod == "fmap":
        # PA is already dropped by dedup; count all remaining fmap entries.
        return "fmap"

    if mod == "dwi":
        return "dwi"

    if mod == "func":
        # Only count bold files
        if not name.endswith("_bold"):
            return None
        m = re.search(r"task-(\w+)_run-\d+_bold$", name)
        if not m:
            return None
        bids_task = m.group(1)
        # Normalise through reverse alias map
        return _BIDS_TO_LABNOTE_CAT.get(bids_task, bids_task)

    return None


def _bids_counts_and_anchors(
    bids_rows: list[dict],
) -> tuple[dict[str, int], dict[str, str]]:
    """
    Returns:
        counts   {category: n_runs}
        anchors  {anchor_cat: first_acq_time_str}
    Anchor cats: T1w, fmap, fLoc, ret (first ret* regardless of task)

    fmap count: use the maximum run number found among fmap entries, because
    the lab note records N fmap pairs and BIDS run IDs should go 1..N.
    """
    counts: dict[str, int] = {c: 0 for c in COUNT_CATS}
    anchors: dict[str, str] = {a: "" for a in ANCHOR_CATS}
    anchor_secs: dict[str, float] = {a: float("inf") for a in ANCHOR_CATS}
    fmap_max_run: int = 0

    for r in bids_rows:
        cat = _bids_row_category(r)
        if cat is None:
            continue

        # fmap: track max run-id instead of incrementing
        if cat == "fmap":
            run_m = re.search(r"_run-(\d+)_", r["name"])
            if run_m:
                fmap_max_run = max(fmap_max_run, int(run_m.group(1)))
        elif cat in counts:
            counts[cat] += 1

        # Anchors
        if cat == "T1w" and r["acq_sec"] < anchor_secs["T1w"]:
            anchor_secs["T1w"] = r["acq_sec"]
            anchors["T1w"] = r["acq_time"]
        if cat == "fmap" and r["acq_sec"] < anchor_secs["fmap"]:
            anchor_secs["fmap"] = r["acq_sec"]
            anchors["fmap"] = r["acq_time"]
        if cat == "fLoc" and r["acq_sec"] < anchor_secs["fLoc"]:
            anchor_secs["fLoc"] = r["acq_sec"]
            anchors["fLoc"] = r["acq_time"]
        if cat in ("retRW", "retFF", "retCB") and r["acq_sec"] < anchor_secs["ret"]:
            anchor_secs["ret"] = r["acq_sec"]
            anchors["ret"] = r["acq_time"]

    counts["fmap"] = fmap_max_run
    return counts, anchors


# ---------------------------------------------------------------------------
# Lab-note loading + normalization (same as show_bids_acqtimes.py)
# ---------------------------------------------------------------------------


def _safe_zfill(v) -> str:
    s = str(v).strip()
    try:
        return str(int(float(s))).zfill(2)
    except (ValueError, TypeError):
        return s


def _clean_subses_df(df) -> object:
    import pandas as pd

    df = df.copy()
    df[["sub", "ses"]] = df[["sub", "ses"]].replace("", pd.NA).ffill()
    df = df.dropna(subset=["sub", "ses"])
    df["sub"] = df["sub"].apply(_safe_zfill)
    df["ses"] = df["ses"].apply(_safe_zfill)
    return df


def _load_labnote_df(xlsx_path: str):
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
    """Strip _rerun-XX, then renumber each family sequentially."""
    for r in rows:
        r["protocol_name"] = re.sub(
            r"_rerun-\d+$", "", r["protocol_name"], flags=re.IGNORECASE
        )
    family_indices: dict[str, list[int]] = {}
    for i, r in enumerate(rows):
        p = r["protocol_name"].strip()
        if re.match(r"fmap_\d+$", p, re.IGNORECASE):
            family_indices.setdefault("fmap", []).append(i)
        else:
            m = re.match(r"([A-Za-z][A-Za-z0-9]*)_run-\d+$", p)
            if m:
                family_indices.setdefault(f"task_{m.group(1)}", []).append(i)

    for family, indices in family_indices.items():
        for new_num, idx in enumerate(indices, start=1):
            p = rows[idx]["protocol_name"]
            if family == "fmap":
                rows[idx]["protocol_name"] = f"fmap_{new_num:02d}"
            else:
                task = re.match(r"([A-Za-z][A-Za-z0-9]*)_run-\d+$", p).group(1)
                rows[idx]["protocol_name"] = f"{task}_run-{new_num:02d}"
    return rows


_FAILED_RE = re.compile(r"^(fail(ed)?|f)$", re.IGNORECASE)

# Families where a failed quality mark means the run should be excluded from
# the expected count (i.e. we don't expect to find it in BIDS)
_QUALITY_GATED_FAMILIES = re.compile(
    r"^(fLoc|retRW|retFF|retCB)_run-\d+$", re.IGNORECASE
)


def _labnote_rows_for(df, sub: str, ses: str) -> list[dict]:
    subset = df[(df["sub"] == sub) & (df["ses"] == ses)]
    rows = []
    for _, row in subset.iterrows():
        proto = str(row.get("protocol_name", "")).strip()
        if not proto or proto.lower() == "nan":
            continue
        ts = row.get("time_start", None)
        qual = str(row.get("quality_mark", "")).strip()
        qual = qual if qual and qual.lower() != "nan" else ""

        # Drop fLoc / ret runs marked as failed — they were not acquired
        # successfully so BIDS should not contain them
        if qual and _FAILED_RE.match(qual) and _QUALITY_GATED_FAMILIES.match(proto):
            continue

        rows.append(
            {
                "protocol_name": proto,
                "time_str": _fmt_time(ts),
                "time_sec": _to_sec(ts),
                "quality": qual,
            }
        )
    return _normalize_labnote_rows(rows)


# ---------------------------------------------------------------------------
# Lab-note → expected counts + anchor times
# ---------------------------------------------------------------------------


def _proto_to_category(protocol_name: str) -> str | None:
    """Map a normalized lab-note protocol_name to a COUNT_CATS key, or None (skip)."""
    p = protocol_name.strip()
    if _SKIP_RE.match(p):
        return None
    if re.match(r"T1", p, re.IGNORECASE):
        return "T1w"
    if re.match(r"T2$", p, re.IGNORECASE):
        return "T2w"
    if re.match(r"fmap_\d+$", p, re.IGNORECASE):
        return "fmap"
    if re.match(r"DWI", p, re.IGNORECASE):
        return "dwi"
    m = re.match(r"([A-Za-z][A-Za-z0-9]*)_run-\d+$", p)
    if m:
        task = m.group(1)
        # All aliases normalise to their lab-note key
        return task  # e.g. "fLoc", "retRW", "retFF", "retCB"
    return None


def _labnote_counts_and_anchors(
    ln_rows: list[dict],
) -> tuple[dict[str, int], dict[str, str]]:
    """
    Returns:
        counts   {category: n_runs}  — how many runs the lab note expects
        anchors  {anchor_cat: time_str}  — first recorded time per anchor
    """
    counts: dict[str, int] = {c: 0 for c in COUNT_CATS}
    anchors: dict[str, str] = {a: "" for a in ANCHOR_CATS}

    for r in ln_rows:
        cat = _proto_to_category(r["protocol_name"])
        if cat is None:
            continue

        if cat in counts:
            counts[cat] += 1

        t = r["time_str"]
        if not t:
            continue

        if cat == "T1w" and not anchors["T1w"]:
            anchors["T1w"] = t
        if cat == "fmap" and not anchors["fmap"]:
            anchors["fmap"] = t
        if cat == "fLoc" and not anchors["fLoc"]:
            anchors["fLoc"] = t
        if cat in ("retRW", "retFF", "retCB") and not anchors["ret"]:
            anchors["ret"] = t

    return counts, anchors


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def _compare_session(
    bids_counts: dict[str, int],
    bids_anchors: dict[str, str],
    ln_counts: dict[str, int],
    ln_anchors: dict[str, str],
) -> tuple[list[dict], list[dict]]:
    """
    Returns:
        count_rows   one dict per COUNT_CATS category
        anchor_rows  one dict per ANCHOR_CATS category
    """
    count_rows: list[dict] = []
    for cat in COUNT_CATS:
        exp = ln_counts.get(cat, 0)
        got = bids_counts.get(cat, 0)
        if exp == 0 and got == 0:
            status = "SKIP"
        elif got == exp:
            status = "MATCH"
        elif got < exp:
            status = "MISSING"
        else:
            status = "EXTRA"
        count_rows.append(
            {
                "category": cat,
                "expected": exp,
                "bids": got,
                "status": status,
            }
        )

    anchor_rows: list[dict] = []
    for anchor in ANCHOR_CATS:
        ln_t = ln_anchors.get(anchor, "")
        bids_t = bids_anchors.get(anchor, "")
        ln_sec = _to_sec(ln_t)
        bids_sec = _to_sec(bids_t)

        if not ln_t or ln_sec == float("inf"):
            status = "NO_LABNOTE_TIME"
            delta = None
        elif not bids_t or bids_sec == float("inf"):
            status = "MISSING"
            delta = None
        else:
            delta = abs(bids_sec - ln_sec)
            status = "MATCH" if delta <= TIME_ERROR_SEC else "WARN"

        anchor_rows.append(
            {
                "anchor": anchor,
                "ln_time": ln_t[:8] if ln_t else "",
                "bids_time": bids_t[:8] if bids_t else "",
                "delta_min": round(delta / 60, 1) if delta is not None else None,
                "status": status,
            }
        )

    return count_rows, anchor_rows


def _session_status(count_rows: list[dict], anchor_rows: list[dict]) -> str:
    statuses = {r["status"] for r in count_rows + anchor_rows}
    if "MISSING" in statuses or "EXTRA" in statuses:
        return "ISSUES"
    if "WARN" in statuses:
        return "WARN"
    return "OK"


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

_STATUS_COLOR = {
    "MATCH": "green",
    "WARN": "yellow",
    "MISSING": "red",
    "EXTRA": "magenta",
    "SKIP": "dim",
    "NO_LABNOTE_TIME": "dim",
}


def _count_cell(row: dict) -> str:
    st = row["status"]
    if st == "SKIP":
        return "[dim]—[/]"
    exp = row["expected"]
    got = row["bids"]
    if st == "MATCH":
        return f"[green]✓ {got}[/]"
    color = _STATUS_COLOR.get(st, "white")
    return f"[{color}]{got}/{exp}[/]"


def _print_session_detail(
    sub: str, ses: str, count_rows: list[dict], anchor_rows: list[dict]
) -> None:
    n_issues = sum(
        1
        for r in count_rows + anchor_rows
        if r["status"] in ("MISSING", "EXTRA", "WARN")
    )
    flag = " [red]✗[/]" if n_issues else " [green]✓[/]"
    console.print(f"\n[bold cyan]sub-{sub}  ses-{ses}[/]{flag}")

    # Count table
    ct = Table(
        title="Run counts  (bids / expected)",
        show_header=True,
        header_style="bold magenta",
        box=None,
        padding=(0, 1),
    )
    ct.add_column("category", style="dim")
    ct.add_column("expected", justify="right")
    ct.add_column("bids", justify="right")
    ct.add_column("status", justify="center")
    for r in count_rows:
        if r["status"] == "SKIP":
            continue
        color = _STATUS_COLOR.get(r["status"], "white")
        ct.add_row(
            r["category"],
            str(r["expected"]),
            str(r["bids"]),
            f"[{color}]{r['status']}[/]",
        )
    console.print(ct)

    # Anchor table
    at = Table(
        title="Anchor times  (first run per category)",
        show_header=True,
        header_style="bold magenta",
        box=None,
        padding=(0, 1),
    )
    at.add_column("anchor", style="dim")
    at.add_column("lab note", justify="right")
    at.add_column("bids", justify="right", style="cyan")
    at.add_column("Δmin", justify="right")
    at.add_column("status", justify="center")
    for r in anchor_rows:
        color = _STATUS_COLOR.get(r["status"], "white")
        delta = str(r["delta_min"]) if r["delta_min"] is not None else ""
        at.add_row(
            r["anchor"],
            r["ln_time"],
            r["bids_time"],
            delta,
            f"[{color}]{r['status']}[/]",
        )
    console.print(at)


def _print_summary_table(results: list[dict]) -> None:
    n_ok = sum(1 for r in results if r["status"] == "OK")
    n_warn = sum(1 for r in results if r["status"] == "WARN")
    n_issues = sum(1 for r in results if r["status"] == "ISSUES")
    n_err = sum(1 for r in results if r["status"] not in ("OK", "WARN", "ISSUES"))

    t = Table(
        title="BIDS counts vs lab-note summary",
        show_header=True,
        header_style="bold magenta",
        box=None,
        padding=(0, 1),
    )
    t.add_column("sub", justify="right", style="cyan")
    t.add_column("ses", justify="right", style="cyan")
    for cat in COUNT_CATS:
        t.add_column(cat, justify="center")
    t.add_column("T1w↑", justify="center")  # anchor time columns
    t.add_column("fmap↑", justify="center")
    t.add_column("fLoc↑", justify="center")
    t.add_column("ret↑", justify="center")
    t.add_column("status", justify="center")

    for r in results:
        count_rows = r.get("count_rows", [])
        anchor_rows = r.get("anchor_rows", [])
        cr_by_cat = {row["category"]: row for row in count_rows}
        ar_by_anch = {row["anchor"]: row for row in anchor_rows}

        count_cells = [
            _count_cell(cr_by_cat[c]) if c in cr_by_cat else "[dim]—[/]"
            for c in COUNT_CATS
        ]
        anchor_cells = []
        for a in ANCHOR_CATS:
            ar = ar_by_anch.get(a, {})
            st = ar.get("status", "")
            bids_t = ar.get("bids_time", "")[:5]  # HH:MM
            delta = ar.get("delta_min")
            if st == "MATCH":
                anchor_cells.append(f"[green]{bids_t}[/]")
            elif st in ("WARN",):
                anchor_cells.append(f"[yellow]{bids_t} Δ{delta}m[/]")
            elif st == "MISSING":
                anchor_cells.append("[red]MISS[/]")
            else:
                anchor_cells.append(f"[dim]{bids_t}[/]")

        st = r["status"]
        if st == "OK":
            st_str = "[green]OK[/]"
        elif st == "WARN":
            st_str = "[yellow]WARN[/]"
        elif st == "ISSUES":
            st_str = "[red]ISSUES[/]"
        else:
            st_str = f"[dim]{st}[/]"

        t.add_row(r["sub"], r["ses"], *count_cells, *anchor_cells, st_str)

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
    os.makedirs(outdir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # summary.tsv
    summary_path = op.join(outdir, "summary.tsv")
    with open(summary_path, "w", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        header = (
            ["sub", "ses", "status"]
            + COUNT_CATS
            + [f"anchor_{a}" for a in ANCHOR_CATS]
            + [f"anchor_{a}_delta_min" for a in ANCHOR_CATS]
        )
        writer.writerow(header)
        for r in results:
            cr = {row["category"]: row for row in r.get("count_rows", [])}
            ar = {row["anchor"]: row for row in r.get("anchor_rows", [])}
            count_vals = [cr.get(c, {}).get("bids", "") for c in COUNT_CATS]
            anchor_vals = [ar.get(a, {}).get("bids_time", "") for a in ANCHOR_CATS]
            delta_vals = [ar.get(a, {}).get("delta_min", "") for a in ANCHOR_CATS]
            writer.writerow(
                [r["sub"], r["ses"], r["status"]]
                + count_vals
                + anchor_vals
                + delta_vals
            )

    # error_subses.tsv
    error_path = op.join(outdir, "error_subses.tsv")
    with open(error_path, "w", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(
            [
                "sub",
                "ses",
                "type",
                "category_or_anchor",
                "expected_or_ln_time",
                "bids_or_bids_time",
                "status",
            ]
        )
        for r in results:
            if r["status"] == "OK":
                continue
            for row in r.get("count_rows", []):
                if row["status"] in ("MATCH", "SKIP"):
                    continue
                writer.writerow(
                    [
                        r["sub"],
                        r["ses"],
                        "count",
                        row["category"],
                        row["expected"],
                        row["bids"],
                        row["status"],
                    ]
                )
            for row in r.get("anchor_rows", []):
                if row["status"] in ("MATCH", "NO_LABNOTE_TIME", "SKIP"):
                    continue
                writer.writerow(
                    [
                        r["sub"],
                        r["ses"],
                        "anchor",
                        row["anchor"],
                        row["ln_time"],
                        row["bids_time"],
                        row["status"],
                    ]
                )

    # detailed_log.txt
    log_path = op.join(outdir, f"detailed_log_{timestamp}.txt")
    capture = Console(file=StringIO(), highlight=False, markup=True)
    for r in results:
        if r.get("count_rows"):
            _render_detail_to(
                capture, r["sub"], r["ses"], r["count_rows"], r["anchor_rows"]
            )
    with open(log_path, "w") as fh:
        fh.write(capture.file.getvalue())

    console.print(f"\n[dim]Output written to {outdir}/[/]")
    console.print(f"  summary    → {op.basename(summary_path)}")
    console.print(f"  errors     → {op.basename(error_path)}")
    console.print(f"  detail log → {op.basename(log_path)}")


def _render_detail_to(
    cap: Console, sub: str, ses: str, count_rows: list[dict], anchor_rows: list[dict]
) -> None:
    cap.print(f"\nsub-{sub}  ses-{ses}")
    cap.print(f"  {'category':<12}  {'expected':>8}  {'bids':>8}  status")
    for r in count_rows:
        if r["status"] == "SKIP":
            continue
        cap.print(
            f"  {r['category']:<12}  {r['expected']:>8}  {r['bids']:>8}  {r['status']}"
        )
    cap.print(
        f"\n  {'anchor':<8}  {'ln_time':>8}  {'bids_time':>9}  {'Δmin':>5}  status"
    )
    for r in anchor_rows:
        delta = f"{r['delta_min']:5.1f}" if r["delta_min"] is not None else "     "
        cap.print(
            f"  {r['anchor']:<8}  {r['ln_time']:>8}  {r['bids_time']:>9}  {delta}  {r['status']}"
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
    labnote: Path = typer.Option(
        ..., "--labnote", "-l", help="Path to lab-note Excel."
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show per-session count + anchor-time tables."
    ),
    outdir: Optional[Path] = typer.Option(
        None,
        "--outdir",
        "-o",
        help="Write summary.tsv, error_subses.tsv, detailed_log.txt here.",
    ),
) -> None:
    """
    Compare BIDS run counts and anchor times against the lab-note spreadsheet.
    """
    # Resolve session pairs
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

    # Load lab note once
    ln_df = _load_labnote_df(str(labnote))
    if ln_df is None:
        raise typer.Exit(1)

    results: list[dict] = []

    for sub, ses in pairs:
        bids_rows = _collect_session(str(bidsdir), sub, ses)
        ln_rows = _labnote_rows_for(ln_df, sub, ses)

        if not ln_rows:
            console.print(f"  [yellow]sub-{sub} ses-{ses}: not found in lab note[/]")
            results.append(
                {
                    "sub": sub,
                    "ses": ses,
                    "status": "NO_LABNOTE",
                    "count_rows": [],
                    "anchor_rows": [],
                }
            )
            continue

        ln_counts, ln_anchors = _labnote_counts_and_anchors(ln_rows)
        bids_counts, bids_anchors = _bids_counts_and_anchors(bids_rows)
        count_rows, anchor_rows = _compare_session(
            bids_counts, bids_anchors, ln_counts, ln_anchors
        )
        status = _session_status(count_rows, anchor_rows)

        results.append(
            {
                "sub": sub,
                "ses": ses,
                "status": status,
                "count_rows": count_rows,
                "anchor_rows": anchor_rows,
            }
        )

        # Streamline: one-liner per session
        n_issues = sum(
            1
            for r in count_rows + anchor_rows
            if r["status"] in ("MISSING", "EXTRA", "WARN")
        )
        flag = (
            "[red]✗[/]"
            if status == "ISSUES"
            else "[yellow]⚠[/]"
            if status == "WARN"
            else "[green]✓[/]"
        )
        console.print(
            f"  {flag}  sub-{sub}  ses-{ses}  [{status}]  {n_issues} issue(s)"
        )

        if verbose:
            _print_session_detail(sub, ses, count_rows, anchor_rows)

    # Summary table (always)
    console.print()
    _print_summary_table(results)

    # Output files
    if outdir is not None:
        _write_outputs(results, str(outdir))


if __name__ == "__main__":
    app()
