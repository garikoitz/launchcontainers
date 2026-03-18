"""
Interactive test for GLMPrepare — paste into IPython or run with:

    ipython -i launchcontainers/tests/test_glm_prepare.py

The BIDSLayout is loaded ONCE at module level.  You can then call the
test_* functions repeatedly without reloading.

Example IPython session::

    %run launchcontainers/tests/test_glm_prepare.py
    mapping = test_mapping_tsv()
    matched = test_bids_symlinks()
    test_fmriprep_symlinks(matched)
    test_full_pipeline()
"""

from __future__ import annotations

import os
import os.path as op

from bids import BIDSLayout

from launchcontainers import utils as do
from launchcontainers.prepare.glm_prepare import GLMPrepare

# ---------------------------------------------------------------------------
# CONFIG — edit these to match your setup
# ---------------------------------------------------------------------------
LC_CONFIG_PATH = "/bcbl/home/public/Gari/VOTCLOC/main_exp/code/glm/lc_config_046.yaml"
SUB = "11"
SES = "03"
OUTPUT_DIR = "/tmp/test_glm_prepare"
# ---------------------------------------------------------------------------

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load config and layout ONCE
lc_config = do.read_yaml(LC_CONFIG_PATH)
bidsdir = op.join(lc_config["general"]["basedir"], lc_config["general"]["bidsdir_name"])

print(f"Loading BIDSLayout from {bidsdir} ...")
layout = BIDSLayout(bidsdir, validate=False)
print(
    f"Layout loaded: {len(layout.get_subjects())} subjects, {len(layout.get_sessions())} sessions."
)

# Instantiate GLMPrepare ONCE
glm = GLMPrepare(lc_config)
print("\nGLMPrepare config:")
print(f"  bidsdir              : {glm.bidsdir}")
print(f"  fmriprep_dir         : {glm.fmriprep_dir}")
print(f"  fmriprep_analysis    : {glm.fmriprep_analysis_name}")
print(f"  space                : {glm.space}")
print(f"  task                 : {glm.task}")
print(f"  start_scans          : {glm.start_scans}")
print(f"  slice_timing_ref     : {glm.slice_timing_ref}")
print(f"  is_WC                : {glm.is_WC}")
print(f"  selected_runs        : {glm.selected_runs}")


# ---------------------------------------------------------------------------
# Test functions — call individually in IPython
# ---------------------------------------------------------------------------


def test_mapping_tsv(sub=SUB, ses=SES):
    """Load (or create) the mapping TSV and print its rows."""
    mapping = glm._load_mapping_tsv(sub, ses)
    print(f"\n--- mapping TSV  sub-{sub} ses-{ses}: {len(mapping)} rows ---")
    for row in mapping:
        print(
            f"  {row['log_file_name']}  task_run={row['task_run']}  "
            f"acq={row['acq_time']}  glm={row['glm_task_run']}"
        )
    return mapping


def test_bids_symlinks(sub=SUB, ses=SES, output_dir=OUTPUT_DIR):
    """Query pyBIDS bold files, match to mapping TSV, create symlinks."""
    matched = glm.gen_bids_bold_symlinks(sub, ses, layout, output_dir=output_dir)
    print(f"\n--- BIDS symlinks  sub-{sub} ses-{ses}: {len(matched)} ---")
    for m in matched:
        print(f"  {op.basename(m['bids_path'])}")
        print(f"    → {op.basename(m['link_path'])}  [{m['glm_task_run']}]")
    return matched


def test_fmriprep_symlinks(bids_matched, sub=SUB, ses=SES, output_dir=OUTPUT_DIR):
    """Create fMRIprep symlinks using the matched list from test_bids_symlinks."""
    links = glm.gen_fmriprep_bold_symlinks(
        sub, ses, bids_matched, output_dir=output_dir
    )
    print(f"\n--- fMRIprep symlinks  sub-{sub} ses-{ses}: {len(links)} ---")
    for lnk in links:
        print(f"  {op.basename(lnk)}  →  {os.readlink(lnk)}")
    return links


def test_events_tsv(sub=SUB, ses=SES, output_dir=OUTPUT_DIR):
    """Write events.tsv from vistadisplog .mat files."""
    written = glm.gen_events_tsv_vistadisplog(sub, ses, output_dir=output_dir)
    print(f"\n--- events.tsv  sub-{sub} ses-{ses}: {len(written)} file(s) ---")
    for f in written:
        print(f"  {op.basename(f)}")
    return written


def test_full_pipeline(sub=SUB, ses=SES):
    """Run the complete WC-GLM prepare for one sub/ses."""
    print(f"\n{'=' * 60}")
    print(f"Full pipeline  sub-{sub}  ses-{ses}")
    print("=" * 60)
    test_events_tsv(sub, ses)
    matched = test_bids_symlinks(sub, ses)
    test_fmriprep_symlinks(matched, sub, ses)
    print(f"\n{'=' * 60}")
    print("Done.")


# ---------------------------------------------------------------------------
# Run a quick smoke-test when executed as a script
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mapping = test_mapping_tsv()
    matched = test_bids_symlinks()
    test_fmriprep_symlinks(matched)
