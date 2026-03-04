#!/usr/bin/env python3
"""
02c_check_session_matches_scanstsv.py

Walks every sub-*/ses-* directory under a BIDS root and validates each
session's scans.tsv against the actual files on disk.

Mapping (BIDS file → scans.tsv entry):
  anat/ _T1w.nii.gz             → _T1_uni.nii.gz
  anat/ _T2w.nii.gz             → _T2w.nii.gz
  fmap/ _epi.nii.gz             → _epi.nii.gz ; run is one digit only (e.g. run-1, not run-01)
  func/ _bold.nii.gz            → _magnitude.nii.gz
  func/ _sbref.nii.gz           → _sbref.nii.gz

  dwi/  _dwi.nii.gz             → _magnitude.nii.gz
  dwi/  acq-magonly_dir-PA or
        acq-nordic_dir-PA*.nii.gz     → acq-floc1d5isodir6_* (or vice versa)
  dwi/  acq-magonly_dir-AP or
        acq-nordic_dir-AP*.nii.gz     → acq-floc1d5isodir104_* (or vice versa)

Ignored tasks: task-WC, task-*retfix*

Output modes (--mode):
  1  simple   — one line per session: PASS / FAIL + error count
  2  detailed — per-file tables printed to console  [default]
  3  pivot    — console summary only; saves pivoted CSV to --log-dir

All modes save a timestamped log to --log-dir (default: ./check_logs).

Usage:
    python check_session.py --bids-dir /path/to/bids
    python check_session.py --bids-dir /path/to/bids --mode 1
    python check_session.py --bids-dir /path/to/bids --mode 3 --log-dir /tmp/logs
    python check_session.py --bids-dir /path/to/bids --sub 10 --ses 03
"""

import re, json, sys, typer
from pathlib import Path
from datetime import datetime
from typing import Optional
import pandas as pd
from rich.console import Console
from rich.table import Table

app     = typer.Typer(add_completion=False)
console = Console()

# ── ignore rules ─────────────────────────────────────────────────────────────
IGNORE_TASK_PATTERNS = [
    re.compile(r'task-WC'),
    re.compile(r'task-\w*retfix\w*'),
]

def should_ignore(filename: str) -> bool:
    return any(p.search(filename) for p in IGNORE_TASK_PATTERNS)

# ── DWI acq equivalence ───────────────────────────────────────────────────────
DWI_BIDS_ACQS = {'magonly', 'nordic'}

# dir-PA  →  floc1d5isodir6
# dir-AP  →  floc1d5isodir104
DWI_DIR_TO_TSV_ACQ: dict[str, str] = {
    'PA': 'floc1d5isodir6',
    'AP': 'floc1d5isodir104',
}

def dwi_tsv_acq_for(filename: str) -> Optional[str]:
    """Return the expected scans.tsv acq label for a DWI BIDS file, based on dir-XX."""
    m = re.search(r'_dir-([^_]+)', filename)
    if m:
        return DWI_DIR_TO_TSV_ACQ.get(m.group(1).upper())
    return None

# ── helpers ───────────────────────────────────────────────────────────────────

def read_json(p: Path) -> dict:
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}

def json_for(nii: Path) -> Path:
    return nii.with_name(nii.name.replace('.nii.gz', '.json'))

def parse_hms(ts: str) -> str:
    """
    Normalize any time string to zero-padded HH:MM:SS.
    Handles:
      '15:41:1.000000'             (no zero-padding, with sub-seconds)
      '9:05:38.297500'             (single-digit hour)
      '2025-06-19T15:41:01.212500' (ISO with sub-seconds)
      '2025-01-28T13:57:43'        (ISO without sub-seconds)
      '13:57:43'                   (plain HH:MM:SS)
    """
    s = str(ts).strip()
    if 'T' in s:
        s = s.split('T')[1]
    s = s.split('.')[0]
    for fmt in ('%H:%M:%S', '%H:%M'):
        try:
            return datetime.strptime(s, fmt).strftime('%H:%M:%S')
        except ValueError:
            continue
    return s

def run_of(fn: str) -> int:
    m = re.search(r'_run-(\d+)', fn)
    return int(m.group(1)) if m else -1

def task_of(fn: str) -> str:
    m = re.search(r'_task-([^_]+)', fn)
    return m.group(1) if m else ''

def series_group(fn: str) -> str:
    mod  = fn.split('/')[0]
    task = re.search(r'_task-([^_]+)', fn)
    dirn = re.search(r'_dir-([^_]+)',  fn)
    stem = re.sub(r'_run-\d+', '', Path(fn).stem)
    suf  = stem.rsplit('_', 1)[-1]
    return '|'.join(filter(None, [
        mod,
        task.group(1) if task else '',
        dirn.group(1) if dirn else '',
        suf,
    ]))

def is_dwi_bids_acq(name: str) -> bool:
    """True when the BIDS file uses one of the known raw DWI acq labels (magonly / nordic)."""
    m = re.search(r'_acq-([^_]+)', name)
    return m is not None and m.group(1) in DWI_BIDS_ACQS

def is_dwi_tsv_acq(name: str) -> bool:
    """True when the scans.tsv filename uses one of the known TSV acq labels (isodir6 / isodir104)."""
    m = re.search(r'_acq-([^_]+)', name)
    return m is not None and any(v in m.group(1) for v in DWI_DIR_TO_TSV_ACQ.values())

def tsv_name_for(bids_name: str, mod: str) -> Optional[str]:
    """BIDS filename → expected scans.tsv filename. None = skip."""
    if mod == 'anat':
        if '_T1w.nii.gz' in bids_name: return bids_name.replace('_T1w.nii.gz', '_T1_uni.nii.gz')
        if '_T2w.nii.gz' in bids_name: return bids_name
    if mod == 'fmap':
        if '_epi.nii.gz' in bids_name: return bids_name
    if mod == 'func':
        if '_bold.nii.gz'  in bids_name: return bids_name.replace('_bold.nii.gz', '_magnitude.nii.gz')
        if '_sbref.nii.gz' in bids_name: return bids_name
    if mod == 'dwi':
        if '_dwi.nii.gz' in bids_name:
            # Step 1: swap suffix _dwi → _magnitude (scans.tsv always uses _magnitude)
            tsv_name = bids_name.replace('_dwi.nii.gz', '_magnitude.nii.gz')
            # Step 2: if the acq label is a raw scanner label (magonly / nordic),
            #         replace it with the direction-appropriate TSV label
            if is_dwi_bids_acq(bids_name):
                tsv_acq = dwi_tsv_acq_for(bids_name)
                if tsv_acq is None:
                    return None   # unknown dir-XX, skip
                tsv_name = re.sub(r'_acq-[^_]+', f'_acq-{tsv_acq}', tsv_name)
            # If the acq label is already a TSV label (floc1d5isodir6 / floc1d5isodir104),
            # the suffix swap above is sufficient — no acq substitution needed.
            return tsv_name
    return None

def bids_candidates_for_tsv(tsv_fn: str) -> list[str]:
    """scans.tsv filename → list of candidate BIDS filenames."""
    bn  = Path(tsv_fn).name
    mod = tsv_fn.split('/')[0]
    if mod == 'anat':
        if '_T1_uni.nii.gz'    in bn: return [bn.replace('_T1_uni.nii.gz', '_T1w.nii.gz')]
        if '_T2w.nii.gz'       in bn: return [bn]
    if mod == 'fmap':
        if '_epi.nii.gz'       in bn: return [bn]
    if mod == 'func':
        if '_magnitude.nii.gz' in bn: return [bn.replace('_magnitude.nii.gz', '_bold.nii.gz')]
        if '_sbref.nii.gz'     in bn: return [bn]
    if mod == 'dwi':
        if '_magnitude.nii.gz' in bn:
            # scans.tsv uses _magnitude.nii.gz but on-disk BIDS files always end
            # in _dwi.nii.gz — swap the suffix first, then build all acq variants.
            base_dwi = bn.replace('_magnitude.nii.gz', '_dwi.nii.gz')

            # keep the TSV acq label as-is (e.g. acq-floc1d5isodir6_dir-PA_dwi)
            tsv_acq_candidate = base_dwi

            # also try the raw scanner acq labels (acq-magonly, acq-nordic),
            # preserving the dir-XX tag in each candidate
            raw_acq_candidates = [
                re.sub(r'_acq-[^_]+', f'_acq-{a}', base_dwi)
                for a in DWI_BIDS_ACQS
            ]

            return [tsv_acq_candidate] + raw_acq_candidates
    return []


# ── time comparison ───────────────────────────────────────────────────────────

def times_match(t1: str, t2: str, max_diff_sec: int = 30) -> bool:
    """
    Compare two BIDS-style AcquisitionTime strings.

    Each SBRef should have been acquired just before its corresponding bold
    run (typically a few seconds to ~1 minute apart). This function checks
    that the absolute difference between two acquisition times is within an
    acceptable window, used to verify sbref↔bold pairing.

    Parameters
    ----------
    t1 : str
        AcquisitionTime string from the sbref JSON, e.g. "14:04:15.487500"
    t2 : str
        AcquisitionTime string from the bold JSON,  e.g. "14:04:29.487500"
    max_diff_sec : int
        Maximum allowed difference in seconds to consider the sbref and bold
        as a matched pair. Default is 30 s; raise to ~180 s if your protocol
        has a longer gap between SBRef and the start of the bold acquisition.

    Returns
    -------
    bool
        True if |t1 - t2| <= max_diff_sec, False otherwise.
    """
    if t1 is None or t2 is None:
        return False

    try:
        dt1 = datetime.strptime(parse_hms(t1), "%H:%M:%S")
        dt2 = datetime.strptime(parse_hms(t2), "%H:%M:%S")
    except ValueError:
        return False

    diff = abs((dt1 - dt2).total_seconds())
    return diff <= max_diff_sec


# ── per-session check ─────────────────────────────────────────────────────────

SCAN_PATTERNS = {
    'anat': ['*_T1w.nii.gz', '*_T2w.nii.gz'],
    'fmap': ['*_epi.nii.gz'],
    'func': ['*_bold.nii.gz', '*_sbref.nii.gz'],
    'dwi':  ['*_dwi.nii.gz', '*_magnitude.nii.gz'],
}

# Maximum seconds between an sbref and its paired bold run
SBREF_BOLD_MAX_DIFF_SEC = 180


def check_session(ses_dir: Path, mode: int) -> tuple[list[str], list[dict]]:
    """
    Run all checks for one session.
    Returns (errors, records) where records are for pivot output.
    """
    errors:  list[str]  = []
    records: list[dict] = []
    label = '/'.join(ses_dir.parts[-2:])

    # ── scans.tsv ──────────────────────────────────────────────────────────────
    tsvs = list(ses_dir.glob("*_scans.tsv"))
    if not tsvs:
        errors.append(f"{label}: no scans.tsv found")
        records.append({'label': label, 'check': 'scans.tsv', 'status': 'MISSING'})
        if mode == 2:
            console.rule(f"[bold cyan]{label}[/bold cyan]")
            console.print("[red]ERROR: no scans.tsv found[/red]\n")
        return errors, records

    df = pd.read_csv(tsvs[0], sep='\t')
    tsv_by_name: dict[str, pd.Series] = {Path(r['filename']).name: r
                                          for _, r in df.iterrows()}

    if mode == 2:
        console.rule(f"[bold cyan]{label}[/bold cyan]")
        mtable = Table(show_header=True, header_style="bold magenta", show_lines=False,
                       title="File-by-file match")
        mtable.add_column("",              width=2, justify="center")
        mtable.add_column("BIDS file",     no_wrap=False)
        mtable.add_column("scans.tsv entry", no_wrap=False)
        mtable.add_column("JSON time",     width=10)
        mtable.add_column("tsv time",      width=10)
        mtable.add_column("Note")

    # ── file-by-file ───────────────────────────────────────────────────────────
    for mod, patterns in SCAN_PATTERNS.items():
        mod_dir = ses_dir / mod
        if not mod_dir.exists():
            continue
        for pattern in patterns:
            for bids_file in sorted(mod_dir.glob(pattern)):
                bname = bids_file.name
                if should_ignore(bname):
                    continue
                expected = tsv_name_for(bname, mod)
                if expected is None:
                    continue

                js_data  = read_json(json_for(bids_file))
                json_acq = parse_hms(js_data['AcquisitionTime']) if js_data.get('AcquisitionTime') else None
                row      = tsv_by_name.get(expected)

                if row is None:
                    err = f"{label}: MISSING in scans.tsv — {mod}/{bname}"
                    errors.append(err)
                    records.append({'label': label, 'check': f'{mod}/{bname}', 'status': 'MISSING'})
                    if mode == 2:
                        mtable.add_row("[red]✗[/red]", f"{mod}/{bname}", expected,
                                       json_acq or "—", "—", "[red]MISSING from scans.tsv[/red]")
                    continue

                tsv_acq = parse_hms(str(row['acq_time']))
                ok = True

                if json_acq is not None and json_acq != tsv_acq:
                    err = f"{label}: acq_time MISMATCH — {mod}/{bname}: JSON={json_acq} tsv={tsv_acq}"
                    errors.append(err)
                    records.append({'label': label, 'check': f'{mod}/{bname}', 'status': 'TIME_MISMATCH'})
                    ok = False
                    if mode == 2:
                        mtable.add_row("[red]✗[/red]", f"{mod}/{bname}", expected,
                                       json_acq, tsv_acq, "[red]acq_time MISMATCH[/red]")
                else:
                    records.append({'label': label, 'check': f'{mod}/{bname}', 'status': 'OK'})
                    if mode == 2:
                        mtable.add_row("[green]✓[/green]", f"{mod}/{bname}", expected,
                                       json_acq or "—", tsv_acq, "")

    # ── orphan check ───────────────────────────────────────────────────────────
    if mode == 2:
        console.print(mtable)
        console.print()
        orphan_table = Table(show_header=True, header_style="bold magenta",
                             title="scans.tsv entries with no BIDS file on disk")
        orphan_table.add_column("scans.tsv entry")
        orphan_table.add_column("Expected BIDS file(s)")
        orphan_count = 0

    for _, row in df.iterrows():
        fn = row['filename']
        if should_ignore(fn):
            continue
        mod        = fn.split('/')[0]
        candidates = bids_candidates_for_tsv(fn)
        if not candidates:
            continue
        found = any((ses_dir / mod / c).exists() for c in candidates)
        if not found:
            err = f"{label}: BIDS file missing on disk — {fn}"
            errors.append(err)
            records.append({'label': label, 'check': fn, 'status': 'BIDS_MISSING'})
            if mode == 2:
                orphan_table.add_row(fn, "  or  ".join(candidates))
                orphan_count += 1

    if mode == 2:
        if orphan_count:
            console.print(orphan_table)
        else:
            console.print("[green]✓ All scans.tsv entries have a matching BIDS file[/green]")
        console.print()

    # ── run-order check ────────────────────────────────────────────────────────
    filtered = df[~df['filename'].apply(should_ignore)]
    run_rows = filtered[filtered['filename'].str.contains(r'_run-\d+', regex=True)].copy()

    if mode == 2:
        run_table = Table(show_header=True, header_style="bold magenta",
                          title="Run-number order (ascending with acq_time)")
        run_table.add_column("",        width=2, justify="center")
        run_table.add_column("Series")
        run_table.add_column("Run order")
        run_table.add_column("Issue")

    if not run_rows.empty:
        run_rows['_dt']  = pd.to_datetime(run_rows['acq_time'], format='ISO8601')
        run_rows['_grp'] = run_rows['filename'].apply(series_group)

        for grp_key, grp in run_rows.groupby('_grp', sort=False):
            grp   = grp.sort_values('_dt')
            runs  = [run_of(f) for f in grp['filename']]
            times = list(grp['_dt'])
            order_str = " → ".join(str(r) for r in runs)
            bad = []
            for i in range(1, len(runs)):
                gap = (times[i] - times[i-1]).total_seconds()
                if runs[i] < runs[i-1] and gap > 30:
                    bad.append(f"run-{runs[i-1]}→run-{runs[i]} @ {times[i].strftime('%H:%M:%S')}")
            if bad:
                err = f"{label}: RUN ORDER — {grp_key}: {'; '.join(bad)}"
                errors.append(err)
                records.append({'label': label, 'check': grp_key, 'status': 'RUN_ORDER'})
                if mode == 2:
                    run_table.add_row("[red]✗[/red]", grp_key, order_str, "; ".join(bad))
            elif mode == 2:
                run_table.add_row("[green]✓[/green]", grp_key, order_str, "")

    if mode == 2:
        console.print(run_table)
        console.print()

    # ── sbref ↔ bold pairing check ────────────────────────────────────────────
    # For every func bold run, verify that a same-task sbref exists in scans.tsv
    # whose acquisition time is within SBREF_BOLD_MAX_DIFF_SEC seconds.
    # The run numbers are NOT required to match (aborted-scan scenario).
    func_dir = ses_dir / 'func'
    if func_dir.exists():
        if mode == 2:
            sbref_table = Table(show_header=True, header_style="bold magenta",
                                title=f"SBRef ↔ Bold pairing (max Δt = {SBREF_BOLD_MAX_DIFF_SEC}s)")
            sbref_table.add_column("",         width=2, justify="center")
            sbref_table.add_column("Bold run",  no_wrap=False)
            sbref_table.add_column("Bold time", width=10)
            sbref_table.add_column("Paired SBRef",  no_wrap=False)
            sbref_table.add_column("SBRef time", width=10)
            sbref_table.add_column("Δt (s)",     width=8, justify="right")
            sbref_table.add_column("Note")

        # Collect all sbref entries from scans.tsv (func only, non-ignored)
        sbref_entries: list[dict] = []
        for _, row in df.iterrows():
            fn = row['filename']
            if fn.startswith('func/') and '_sbref.nii.gz' in fn and not should_ignore(fn):
                sbref_entries.append({
                    'filename': fn,
                    'run':      run_of(fn),
                    'task':     task_of(fn),
                    'acq_time': str(row['acq_time']),
                })

        # Check each bold file
        for bold_file in sorted(func_dir.glob('*_bold.nii.gz')):
            bname = bold_file.name
            if should_ignore(bname):
                continue

            bold_run  = run_of(bname)
            bold_task = task_of(bname)

            bold_js   = read_json(json_for(bold_file))
            bold_time = bold_js.get('AcquisitionTime')

            # Candidate sbrefs: same task (run number may differ)
            candidates = [e for e in sbref_entries if e['task'] == bold_task]

            if not candidates:
                err = f"{label}: NO SBREF — no sbref found in scans.tsv for task-{bold_task} (bold run-{bold_run:02d})"
                errors.append(err)
                records.append({'label': label,
                                'check': f'func/{bname}',
                                'status': 'SBREF_MISSING'})
                if mode == 2:
                    sbref_table.add_row(
                        "[red]✗[/red]", f"func/{bname}",
                        parse_hms(bold_time) if bold_time else "—",
                        "—", "—", "—",
                        "[red]no sbref for this task[/red]")
                continue

            # Find the best-matching sbref by temporal proximity
            best        = None
            best_diff   = float('inf')
            best_stime  = None
            for cand in candidates:
                cand_time = cand['acq_time']
                # Compute raw diff in seconds for ranking
                try:
                    dt_b = datetime.strptime(parse_hms(bold_time), "%H:%M:%S") if bold_time else None
                    dt_s = datetime.strptime(parse_hms(cand_time), "%H:%M:%S")
                    diff = abs((dt_b - dt_s).total_seconds()) if dt_b else float('inf')
                except ValueError:
                    diff = float('inf')
                if diff < best_diff:
                    best       = cand
                    best_diff  = diff
                    best_stime = cand_time

            matched = times_match(best_stime, bold_time, SBREF_BOLD_MAX_DIFF_SEC) if best else False
            bold_time_str  = parse_hms(bold_time)  if bold_time  else "—"
            sbref_time_str = parse_hms(best_stime) if best_stime else "—"
            diff_str       = f"{int(best_diff)}" if best_diff < float('inf') else "—"
            paired_name    = f"func/{best['filename'].split('/')[-1]}" if best else "—"

            if not matched:
                err = (f"{label}: SBREF MISMATCH — bold func/{bname} (run-{bold_run:02d}, "
                       f"{bold_time_str}) has no sbref within {SBREF_BOLD_MAX_DIFF_SEC}s "
                       f"(closest: {paired_name} @ {sbref_time_str}, Δt={diff_str}s)")
                errors.append(err)
                records.append({'label': label,
                                'check': f'func/{bname}',
                                'status': 'SBREF_TIME_MISMATCH'})
                if mode == 2:
                    sbref_table.add_row(
                        "[red]✗[/red]", f"func/{bname}", bold_time_str,
                        paired_name, sbref_time_str, diff_str,
                        f"[red]Δt > {SBREF_BOLD_MAX_DIFF_SEC}s[/red]")
            else:
                records.append({'label': label,
                                'check': f'func/{bname}',
                                'status': 'OK'})
                if mode == 2:
                    sbref_table.add_row(
                        "[green]✓[/green]", f"func/{bname}", bold_time_str,
                        paired_name, sbref_time_str, diff_str, "")

        if mode == 2:
            console.print(sbref_table)
            console.print()

    return errors, records


# ── output helpers ────────────────────────────────────────────────────────────

def output_simple(ses_dirs, all_errors):
    """Mode 1: one line per session."""
    console.rule("[bold]Results[/bold]")
    for ses_dir in ses_dirs:
        label = '/'.join(ses_dir.parts[-2:])
        errs  = all_errors.get(label, [])
        if errs:
            console.print(f"  [red]✗ FAIL[/red]  {label}  ({len(errs)} error(s))")
        else:
            console.print(f"  [green]✓ PASS[/green]  {label}")

def output_pivot(ses_dirs, all_errors, log_dir: Path, ts: str) -> Path:
    """Mode 3: pivot table sub × ses → PASS/FAIL."""
    rows = []
    for ses_dir in ses_dirs:
        label = '/'.join(ses_dir.parts[-2:])
        sub   = ses_dir.parts[-2]
        ses   = ses_dir.parts[-1]
        errs  = all_errors.get(label, [])
        status = 'FAIL' if errs else 'PASS'
        n_err  = len(errs)
        rows.append({'sub': sub, 'ses': ses, 'status': status, 'n_errors': n_err})

    df = pd.DataFrame(rows)

    # Simple pass/fail pivot
    pivot = df.pivot(index='sub', columns='ses', values='status').fillna('—')

    console.rule("[bold]Pivot: sub × ses[/bold]")
    ptable = Table(show_header=True, header_style="bold magenta")
    ptable.add_column("sub \\ ses")
    ses_cols = list(pivot.columns)
    for c in ses_cols:
        ptable.add_column(c, justify="center")
    for sub_val, row in pivot.iterrows():
        cells = []
        for c in ses_cols:
            v = row[c]
            cells.append("[green]PASS[/green]" if v == 'PASS' else
                         "[red]FAIL[/red]"     if v == 'FAIL' else "—")
        ptable.add_row(sub_val, *cells)
    console.print(ptable)

    # Save pivot CSV
    csv_path = log_dir / f"check_session_pivot_{ts}.csv"
    pivot.to_csv(csv_path)
    console.print(f"\n[cyan]Pivot CSV → {csv_path}[/cyan]")
    return csv_path


# ── main ──────────────────────────────────────────────────────────────────────

@app.command()
def main(
    bids_dir: Path = typer.Option(...,   "--bids-dir", "-b", help="BIDS root directory"),
    sub:      str  = typer.Option(None,  "--sub",             help="Filter subject, e.g. 01"),
    ses:      str  = typer.Option(None,  "--ses",             help="Filter session, e.g. 03"),
    mode:     int  = typer.Option(2,     "--mode", "-m",
                                   help="Output mode: 1=simple  2=detailed  3=pivot"),
    log_dir:  Path = typer.Option(Path("check_logs"), "--log-dir", "-l",
                                   help="Directory to store log files"),
):
    """Validate BIDS sessions against their scans.tsv files."""

    if not bids_dir.exists():
        console.print(f"[red]BIDS dir not found: {bids_dir}[/red]"); raise typer.Exit(1)
    if mode not in (1, 2, 3):
        console.print("[red]--mode must be 1, 2, or 3[/red]"); raise typer.Exit(1)

    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')

    pattern = f"sub-{sub}/ses-{ses}" if sub and ses else \
              f"sub-{sub}/ses-*"     if sub          else \
              f"sub-*/ses-{ses}"     if ses           else \
              "sub-*/ses-*"
    ses_dirs = sorted(bids_dir.glob(pattern))

    if not ses_dirs:
        console.print(f"[red]No sessions found: {bids_dir}/{pattern}[/red]"); raise typer.Exit(1)

    console.print(f"\n[bold cyan]BIDS root : {bids_dir}[/bold cyan]")
    console.print(f"Sessions  : {len(ses_dirs)}  |  mode={mode}  |  logs → {log_dir}\n")

    all_errors:  dict[str, list[str]]  = {}
    all_records: list[dict]            = []

    # Run checks (mode 2 prints detail inline; modes 1 & 3 are quiet per-session)
    for ses_dir in ses_dirs:
        errs, recs = check_session(ses_dir, mode)
        label = '/'.join(ses_dir.parts[-2:])
        if errs:
            all_errors[label] = errs
        all_records.extend(recs)

    # ── output ─────────────────────────────────────────────────────────────────
    if mode == 1:
        output_simple(ses_dirs, all_errors)

    if mode == 3:
        output_pivot(ses_dirs, all_errors, log_dir, ts)

    # ── summary (all modes) ────────────────────────────────────────────────────
    console.rule("[bold]Overall Summary[/bold]")
    total   = len(ses_dirs)
    failed  = len(all_errors)
    passed  = total - failed
    console.print(f"\n  Checked : {total}")
    console.print(f"  [green]Passed  : {passed}[/green]")
    if failed:
        console.print(f"  [red]Failed  : {failed}[/red]")
        if mode != 2:   # mode 2 already printed details inline
            console.print()
            for label, errs in all_errors.items():
                console.print(f"  [red]{label}[/red]  ({len(errs)} error(s)):")
                for e in errs:
                    console.print(f"    [red]•[/red] {e[len(label)+2:]}")
    else:
        console.print(f"  Failed  : {failed}")
        console.print(f"\n[green]✓ All sessions clean![/green]")

    # ── save log ───────────────────────────────────────────────────────────────
    log_path = log_dir / f"check_session_{ts}.txt"
    with open(log_path, 'w') as f:
        f.write(f"check_session log  {ts}\n")
        f.write(f"BIDS root : {bids_dir}\n")
        f.write(f"Sessions  : {total}  passed={passed}  failed={failed}\n\n")
        if all_errors:
            for label, errs in all_errors.items():
                f.write(f"{label}:\n")
                for e in errs:
                    f.write(f"  {e}\n")
                f.write("\n")
        else:
            f.write("All sessions passed.\n")

    # Also save detailed records CSV (useful for any mode)
    rec_path = log_dir / f"check_session_records_{ts}.csv"
    pd.DataFrame(all_records).to_csv(rec_path, index=False)

    console.print(f"\n[cyan]Log   → {log_path}[/cyan]")
    console.print(f"[cyan]CSV   → {rec_path}[/cyan]")

    sys.exit(1 if all_errors else 0)


if __name__ == "__main__":
    app()