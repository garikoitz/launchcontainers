from __future__ import annotations

import os

import pandas as pd
from bids import BIDSLayout

# Define the fMRIPrep derivatives directory
bids_dir = '/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS'
fp_ana_name = 'beforeMar05_US'
fmriprep_dir = os.path.join(
    bids_dir, 'derivatives',
    f'fmriprep-{fp_ana_name}',
)  # Update with actual path

# Load fMRIPrep layout
layout = BIDSLayout(fmriprep_dir, derivatives=True, validate=False)

# Get subjects and sessions processed by fMRIPrep
subjects = layout.get_subjects()
sessions = layout.get_sessions()

# Create a list of (sub, ses) pairs
sub_ses_pairs = []
for sub in subjects:
    ses_list = layout.get_sessions(subject=sub)
    if not ses_list:  # If no session folder, assume single-session (BIDS convention)
        ses_list = ['N/A']  # Mark as "N/A" for single-session subjects
    for ses in ses_list:
        sub_ses_pairs.append((sub, ses))

# Convert to DataFrame
df_fmriprep = pd.DataFrame(sub_ses_pairs, columns=['sub', 'ses'])
output_txt = os.path.join(bids_dir, 'code', f'subseslist_{fp_ana_name}.txt')
df_fmriprep.to_csv(output_txt, sep='\t', index=False)
