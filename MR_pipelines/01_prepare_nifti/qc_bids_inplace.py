#!/usr/bin/env python3
import os
import pandas as pd
from bids import BIDSLayout

# Paths
basedir = '/bcbl/home/public/Gari/VOTCLOC/main_exp'
bids_dir = os.path.join(basedir, 'BIDS')
fmriprep_dir = os.path.join(bids_dir, 'derivatives', 'fmriprep-25.1.4_t2_pial_dummyscan_5')
# Build layouts
# Use bids_dir (not basedir) for the raw BIDS layout; turn off validation for speed/robustness
bids_layout = BIDSLayout(bids_dir, validate=False, derivatives=False)
fmriprep_layout = BIDSLayout(fmriprep_dir, validate=False)

# define output csv
out_csv = os.path.join(basedir, 'bids_qc_summary.csv')


# Helper to count unique runs for a set of files, ignoring duplicate echoes, etc.
def count_unique_runs(files, layout):
    """
    Count unique runs among files. If no run entity is present,
    fall back to counting unique (task, acq, dir) tuples to avoid
    overcounting multi-echo or other duplicates.
    """
    if not files:
        return 0
    unique = set()
    for f in files:
        ents = layout.parse_file_entities(f)
        # Prefer run if available; otherwise use a tuple of common entities
        if 'run' in ents and ents['run'] is not None:
            key = ('run', ents.get('run'))
        else:
            key = (
                'fallback',
                ents.get('task'),
                ents.get('acq'),
                ents.get('dir'),
                ents.get('rec'),
                ents.get('echo'),  # include echo so distinct acquisitions count once per combo
            )
        unique.add(key)
    return len(unique)

rows = []

# Enumerate subjects
subjects = bids_layout.get_subjects()
subjects = sorted(subjects)

for sub in subjects:
    # Sessions can vary per subject; if none, treat as a single "NA" session
    sessions = bids_layout.get_sessions(subject=sub)
    if not sessions:
        sessions = [None]

    for ses in sorted(sessions) if all(s is not None for s in sessions) else sessions:
        # ---------- Presence checks ----------
        # Any anat/fmap/func files?
        has_anat_any = bool(bids_layout.get(subject=sub, session=ses, datatype='anat', return_type='filename'))
        has_fmap_any = bool(bids_layout.get(subject=sub, session=ses, datatype='fmap', return_type='filename'))
        has_func_any = bool(bids_layout.get(subject=sub, session=ses, datatype='func', return_type='filename'))

        # ---------- Anat: require BOTH T1w and T2w ----------
        t1_files = bids_layout.get(subject=sub, session=ses, datatype='anat', suffix='T1w', extension=['.nii', '.nii.gz'], return_type='filename')
        t2_files = bids_layout.get(subject=sub, session=ses, datatype='anat', suffix='T2w', extension=['.nii', '.nii.gz'], return_type='filename')
        anat_ok = bool(t1_files) and bool(t2_files)

        # If there are anat files but not both T1w & T2w, we still report anat=False to match your spec.
        # (You can change to 'has_anat_any' if you prefer "any anat present".)

        # ---------- Func: count fLoc runs ----------
        floc_files = bids_layout.get(
            subject=sub, session=ses, datatype='func',
            task='fLoc', suffix='bold',
            extension=['.nii', '.nii.gz'], return_type='filename'
        )
        n_floc = count_unique_runs(floc_files, bids_layout) if has_func_any else 0


        # ---------- Assemble row ----------
        rows.append({
            'sub': str(sub),
            'ses': str(ses) if ses is not None else 'NA',
            'anat': bool(anat_ok),
            'fmap': bool(has_fmap_any),
            'func_floc': int(n_floc)
        })

# Save CSV
df = pd.DataFrame(rows).sort_values(['sub', 'ses'])
df.to_csv(out_csv, index=False)

print(f"Wrote QC summary to: {out_csv}")
print(df.head(20).to_string(index=False))
