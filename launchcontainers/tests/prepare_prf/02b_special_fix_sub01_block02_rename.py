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

Actions performed (ses-09):
    1. BIDS/func         — move block02_run-01 files to backup dir
    2. BIDS/fmap         — remove block02_run-01 entries from IntendedFor
    3. fmriprep/func     — move block02_run-01 files to backup dir
    4. fmriprep/fmap     — remove block02_run-01 entries from IntendedFor
       (auto00022_desc-preproc_fieldmap.json)

Set DRY_RUN = False to execute. Default is True (preview only).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DRY_RUN = True   # ← set to False to actually run

BIDS     = Path("/scratch/tlei/VOTCLOC/BIDS")
FMRIPREP = BIDS / "derivatives" / "fmriprep" / "analysis-25.1.4_t2w_fmapsbref_newest"

SUB      = "01"
SESSIONS = ["09", "10"]

OLD_TASK = "retfixRWblock02"
OLD_RUN  = "run-01"
OLD_STEM = f"task-{OLD_TASK}_{OLD_RUN}"   # substring matched in filenames & IntendedFor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _log(msg: str) -> None:
    prefix = "[DRY-RUN] " if DRY_RUN else "[ACTION]  "
    print(prefix + msg)


def move_to_backup(path: Path, backup_root: Path, root: Path) -> None:
    backup_root.mkdir(parents=True, exist_ok=True)
    dest = backup_root / path.name
    _log(f"mv  {path.relative_to(root)}  →  {dest.relative_to(root)}")
    if not DRY_RUN:
        shutil.move(str(path), str(dest))


def patch_intendedfor(json_path: Path, old_stem: str, backup_root: Path, root: Path) -> int:
    """Remove IntendedFor entries containing old_stem. Returns number removed."""
    data = json.loads(json_path.read_text())
    intended = data.get("IntendedFor", [])
    to_remove = [e for e in intended if old_stem in e]
    if not to_remove:
        return 0

    backup_root.mkdir(parents=True, exist_ok=True)
    bak = backup_root / (json_path.name + ".bak")
    _log(f"backup JSON  →  {bak.relative_to(root)}")
    if not DRY_RUN:
        shutil.copy2(str(json_path), str(bak))

    for e in to_remove:
        _log(f"  remove IntendedFor: {e}")
    _log(f"  patch  {json_path.relative_to(root)}  ({len(to_remove)} entr(ies) removed)")
    if not DRY_RUN:
        data["IntendedFor"] = [e for e in intended if old_stem not in e]
        json_path.write_text(json.dumps(data, indent=4))

    return len(to_remove)


# ---------------------------------------------------------------------------
# Per-session checks
# ---------------------------------------------------------------------------

def fix_session(ses: str) -> dict:
    """Run all 4 checks for one session. Returns counts per area."""
    print(f"\n{'='*70}")
    print(f"  sub-{SUB}  ses-{ses}")
    print(f"{'='*70}")

    counts = {"bids_func": 0, "bids_fmap": 0, "fp_func": 0, "fp_fmap": 0}

    # 1. BIDS/func ---------------------------------------------------------
    bids_func   = BIDS / f"sub-{SUB}" / f"ses-{ses}" / "func"
    bids_f_bak  = BIDS / f"sub-{SUB}" / f"ses-{ses}" / "func_backup_block02_run01"

    print(f"\n[1] BIDS/func:     {bids_func}")
    found = sorted(bids_func.glob(f"*{OLD_STEM}*")) if bids_func.exists() else []
    if found:
        for f in found:
            move_to_backup(f, bids_f_bak, BIDS)
        counts["bids_func"] = len(found)
    else:
        print("    ✓ clean — no block02_run-01 files")

    # 2. BIDS/fmap ---------------------------------------------------------
    bids_fmap   = BIDS / f"sub-{SUB}" / f"ses-{ses}" / "fmap"
    bids_fm_bak = BIDS / f"sub-{SUB}" / f"ses-{ses}" / "fmap_backup_block02_run01"

    print(f"\n[2] BIDS/fmap:     {bids_fmap}")
    n = 0
    for j in sorted(bids_fmap.glob("*.json")):
        n += patch_intendedfor(j, OLD_STEM, bids_fm_bak, BIDS)
    if n == 0:
        print("    ✓ clean — no block02_run-01 in IntendedFor")
    counts["bids_fmap"] = n

    # 3. fmriprep/func -----------------------------------------------------
    fp_func   = FMRIPREP / f"sub-{SUB}" / f"ses-{ses}" / "func"
    fp_f_bak  = FMRIPREP / f"sub-{SUB}" / f"ses-{ses}" / "func_backup_block02_run01"

    print(f"\n[3] fmriprep/func: {fp_func}")
    found = sorted(fp_func.glob(f"*{OLD_STEM}*")) if fp_func.exists() else []
    if found:
        for f in found:
            move_to_backup(f, fp_f_bak, BIDS)
        counts["fp_func"] = len(found)
    else:
        print("    ✓ clean — no block02_run-01 files")

    # 4. fmriprep/fmap -----------------------------------------------------
    fp_fmap   = FMRIPREP / f"sub-{SUB}" / f"ses-{ses}" / "fmap"
    fp_fm_bak = FMRIPREP / f"sub-{SUB}" / f"ses-{ses}" / "fmap_backup_block02_run01"

    print(f"\n[4] fmriprep/fmap: {fp_fmap}")
    n = 0
    if fp_fmap.exists():
        for j in sorted(fp_fmap.glob("*.json")):
            n += patch_intendedfor(j, OLD_STEM, fp_fm_bak, BIDS)
    if n == 0:
        print("    ✓ clean — no block02_run-01 in IntendedFor")
    counts["fp_fmap"] = n

    return counts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 70)
    print(f"Special fix: sub-{SUB}  |  remove {OLD_STEM}")
    print(f"Sessions: {SESSIONS}")
    print(f"DRY_RUN = {DRY_RUN}")
    print("=" * 70)

    totals = {"bids_func": 0, "bids_fmap": 0, "fp_func": 0, "fp_fmap": 0}
    for ses in SESSIONS:
        c = fix_session(ses)
        for k in totals:
            totals[k] += c[k]

    print(f"\n{'='*70}")
    print("Summary:")
    print(f"  BIDS/func     files moved  : {totals['bids_func']}")
    print(f"  BIDS/fmap     entries removed: {totals['bids_fmap']}")
    print(f"  fmriprep/func files moved  : {totals['fp_func']}")
    print(f"  fmriprep/fmap entries removed: {totals['fp_fmap']}")
    if DRY_RUN:
        print("\nSet DRY_RUN = False and re-run to apply.")
    print("=" * 70)


if __name__ == "__main__":
    main()
