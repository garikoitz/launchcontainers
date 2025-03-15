from __future__ import annotations

import pandas as pd
from bids import BIDSLayout

# Load BIDS dataset (modify path to your BIDS root)
bids_dir = '/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS'
layout = BIDSLayout(bids_dir, validate=False)

# Define the task name
task_name = 'fLoc'

# Get all subject-session-run combinations with BOLD data
bold_files = layout.get(task=task_name, suffix='bold', extension='nii.gz', return_type='filename')

bold_entries = set()
for f in bold_files:
    ent = layout.parse_file_entities(f)
    bold_entries.add((ent['subject'], ent.get('session', 'N/A'), ent['run']))

# Get all subject-session-run combinations with complete events.tsv
event_files = layout.get(task=task_name, suffix='events', extension='tsv', return_type='filename')

event_entries = set()
for f in event_files:
    ent = layout.parse_file_entities(f)
    event_entries.add((ent['subject'], ent.get('session', 'N/A'), ent['run']))

# Find mismatches
bold_without_events = bold_entries - event_entries
events_without_bold = event_entries - bold_entries

# Convert to DataFrame for easy viewing
df_mismatch = pd.DataFrame(
    list(bold_without_events),
    columns=['sub', 'ses', 'run'],
)
