# -----------------------------------------------------------------------------
# Copyright (c) Yongning Lei 2025
# All rights reserved.
#
# This script is distributed under the Apache-2.0 license.
# You may use, distribute, and modify this code under the terms of the Apache-2.0 license.
# See the LICENSE file for details.
#
# THIS SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE, AND NONINFRINGEMENT.
#
# Author: Yongning Lei
# Email: yl4874@nyu.edu
# GitHub: https://github.com/yongninglei
# -----------------------------------------------------------------------------

'''
This code is to seperate prf nifti from BIDS, raw_nifti and fmripre derivatives folder

First, nifti in BIDS will be calculated with mix of fMRI and ret.

Then, once fMRIprep finished

For BIDS/sub/ses/func folder, 
it will sep each session into ses-01loc and ses-01ret, and also rename the corresponding file name

For derivatives/fmriprep/analysis/sub/ses/func folder, 
it will sep each session into ses-01loc and ses-01ret, and also rename the corresponding file name

For BIDS/sub/ses/anat and fmap,
It will create a symlink from ses-ret to ses-loc, ses-log will be the major folder
'''

"""
Split single-session localizer/ret data into two sessions (ses-01loc, ses-01ret)
for both BIDS and fMRIPrep directories using the same function.

Usage:
  1. Update the user settings under "CONFIGURATION" below.
  2. Run: python separate_prf_nifti.py

Result:
  - BIDS/sub-XX/ses-01loc and BIDS/sub-XX/ses-01ret, each with its own func/
    containing localizer or ret runs. The original ses-01/func/ data are moved
    accordingly.
  - In BIDS only: anat/ and fmap/ in ses-01ret will be symlinks to ses-01loc.
  - The same reorganization occurs in the derivatives/fmriprep folder (but no
    symlinks created).
  - BIDS validator runs on the reorganized BIDS dataset at the end.
"""

import os
import shutil
import subprocess
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================
BIDS_ROOT = Path("/path/to/BIDS")
DERIV_ROOT = Path("/path/to/derivatives/fmriprep")
sub_id = ["03"]  # e.g. ["sub-01", "sub-02", ...]
ses_id = "01"
SUBJECT=[f'sub-{i}' for i in sub_id]
OLD_SESSION=f'ses-{ses_id}'
NEW_LOC_SESSION = f'ses-{ses_id}loc'
NEW_RET_SESSION = f'ses-{ses_id}ret'

LOCALIZER_KEY = "localizer"  # If your filenames contain "localizer"
RET_KEY = "ret"              # If your filenames contain "ret"

# Whether you want the final BIDS dataset validated
RUN_BIDS_VALIDATOR = True

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def move_and_rename_task_files(
    root_dir: Path,
    subject_id: str,
    old_ses: str,
    new_ses: str,
    task_key: str,
    new_task_label: str
):
    """
    Move/rename files under func/ from one session folder to a new session folder,
    replacing the old_ses in filenames with new_ses, and the old task_key
    with new_task_label.

    This function applies to either BIDS or fMRIPrep directories, as they share
    the same sub-XX/ses-YY/func structure.

    Parameters
    ----------
    root_dir : Path
        Root of BIDS or derivatives folder.
    subject_id : str
        Subject identifier, e.g. "sub-01".
    old_ses : str
        Original session name, e.g. "ses-01".
    new_ses : str
        New session name, e.g. "ses-01loc".
    task_key : str
        A string identifying which task to match in the filenames (e.g. "localizer").
    new_task_label : str
        What to rename the matched task_key to (e.g. "localizer" or "ret").
    """

    old_func_dir = root_dir / subject_id / old_ses / "func"
    new_func_dir = root_dir / subject_id / new_ses / "func"

    if not old_func_dir.exists():
        # There's no data for this session; skip
        print(f"  [WARNING] {old_func_dir} not found. Skipping.")
        return

    new_func_dir.mkdir(parents=True, exist_ok=True)

    # Move all files in old_func_dir whose filename contains the task_key.
    for item in old_func_dir.glob("*"):
        if not item.is_file():
            continue
        if task_key not in item.name:
            continue

        # Construct new filename
        # e.g. sub-01_ses-01_task-localizer_bold.nii.gz
        # becomes sub-01_ses-01loc_task-localizer_bold.nii.gz
        # then we also rename the "localizer" substring if needed
        new_name = item.name
        new_name = new_name.replace(old_ses, new_ses)

        # If you need to specifically replace only the "task-localizer" portion,
        # you could do more targeted string replacement. For simplicity, we
        # assume the presence of "localizer" is unique.
        new_name = new_name.replace(task_key, new_task_label)

        new_path = new_func_dir / new_name
        print(f"  Moving {item} -> {new_path}")
        shutil.move(str(item), str(new_path))


def create_symlinks_for_anat_fmap(
    bids_root: Path,
    subject_id: str,
    loc_ses: str,
    ret_ses: str
):
    """
    For the BIDS folder only: create symlinks in ses-01ret/anat and ses-01ret/fmap
    pointing to the actual files in ses-01loc. The assumption is that 'loc' is
    the "primary" data location for anatomical and field maps.

    Parameters
    ----------
    bids_root : Path
        Path to BIDS root directory.
    subject_id : str
        Subject ID (e.g. "sub-01").
    loc_ses : str
        Localizer session name, e.g. "ses-01loc".
    ret_ses : str
        Ret session name, e.g. "ses-01ret".
    """

    for modality in ["anat", "fmap"]:
        loc_dir = bids_root / subject_id / loc_ses / modality
        ret_dir = bids_root / subject_id / ret_ses / modality

        if not loc_dir.exists():
            print(f"  [WARNING] {loc_dir} not found; skipping symlink creation.")
            continue

        ret_dir.mkdir(parents=True, exist_ok=True)

        for f in loc_dir.glob("*"):
            if not f.is_file():
                continue
            symlink_path = ret_dir / f.name
            if symlink_path.exists():
                symlink_path.unlink()
            # Create a relative symlink so it’s easily portable
            target_rel_path = os.path.relpath(f, start=ret_dir)
            print(f"  Creating symlink: {symlink_path} -> {target_rel_path}")
            symlink_path.symlink_to(target_rel_path)


def run_bids_validator(bids_dir: Path):
    """
    Run the BIDS validator on the BIDS dataset.
    """
    print("\n==> Running BIDS Validator...\n")
    try:
        subprocess.run(["bids-validator", str(bids_dir)], check=True)
    except FileNotFoundError:
        print("  [ERROR] bids-validator not found on PATH. Please install it.")
    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] BIDS validator exited with code {e.returncode}.")


def split_sessions_for_sub(
    root_dir: Path,
    subject_id: str,
    old_ses: str,
    loc_ses: str,
    ret_ses: str,
    localizer_key: str,
    ret_key: str,
    create_symlinks=False
):
    """
    High-level function that splits the old_ses into loc_ses and ret_ses
    for a single subject directory under 'root_dir'. This function:

      1. Moves localizer task files from old_ses/func to loc_ses/func
      2. Moves ret task files from old_ses/func to ret_ses/func
      3. Optionally creates symlinks for anat/fmap from loc_ses to ret_ses

    Works for either the BIDS folder or the fMRIPrep folder, depending on
    what 'root_dir' you pass.

    Parameters
    ----------
    root_dir : Path
        e.g. the BIDS root or the derivatives/fmriprep root
    subject_id : str
        e.g. "sub-01"
    old_ses : str
        e.g. "ses-01"
    loc_ses : str
        e.g. "ses-01loc"
    ret_ses : str
        e.g. "ses-01ret"
    localizer_key : str
        e.g. "localizer" (to find relevant files in old_ses)
    ret_key : str
        e.g. "ret"
    create_symlinks : bool
        If True, create symlinks in anat/fmap of ret_ses → loc_ses (intended for BIDS).
    """

    print(f"\n--- Splitting data in {root_dir} for {subject_id} ---")

    # Move/rename localizer
    move_and_rename_task_files(
        root_dir, subject_id, old_ses, loc_ses, localizer_key, "localizer"
    )

    # Move/rename ret
    move_and_rename_task_files(
        root_dir, subject_id, old_ses, ret_ses, ret_key, "ret"
    )

    # Optionally create symlinks (anat, fmap)
    if create_symlinks:
        create_symlinks_for_anat_fmap(
            bids_root=root_dir,
            subject_id=subject_id,
            loc_ses=loc_ses,
            ret_ses=ret_ses
        )


def main():
    # ========================================================================
    # 1) Reorganize BIDS folder
    # ========================================================================
    for sub_id in SUBJECTS:
        split_sessions_for_sub(
            root_dir=BIDS_ROOT,
            subject_id=sub_id,
            old_ses=OLD_SESSION,
            loc_ses=NEW_LOC_SESSION,
            ret_ses=NEW_RET_SESSION,
            localizer_key=LOCALIZER_KEY,
            ret_key=RET_KEY,
            create_symlinks=True  # BIDS only
        )

    # ========================================================================
    # 2) Reorganize derivatives/fMRIPrep folder (same structure, no symlinks)
    # ========================================================================
    for sub_id in SUBJECTS:
        split_sessions_for_sub(
            root_dir=DERIV_ROOT,
            subject_id=sub_id,
            old_ses=OLD_SESSION,
            loc_ses=NEW_LOC_SESSION,
            ret_ses=NEW_RET_SESSION,
            localizer_key=LOCALIZER_KEY,
            ret_key=RET_KEY,
            create_symlinks=False  # Typically no symlinks in derivatives
        )

    # ========================================================================
    # 3) Validate final BIDS structure
    # ========================================================================
    if RUN_BIDS_VALIDATOR:
        run_bids_validator(BIDS_ROOT)


if __name__ == "__main__":
    main()
