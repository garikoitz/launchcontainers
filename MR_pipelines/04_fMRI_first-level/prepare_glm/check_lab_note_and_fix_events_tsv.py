from __future__ import annotations

import os

import pandas as pd
from bids import BIDSLayout

# 🔹 Path to the downloaded Excel file
# Replace with your actual file path
xlsx_file = '/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS/sourcedata/VOTCLOC_subject_list.xlsx'
# 🔹 Load the Excel file
xls = pd.ExcelFile(xlsx_file)

sub='11'
ses='02'
# 🔹 Find all sheets that match "sub-xx"
filtered_data = []
for sheet_name in xls.sheet_names:
    if sheet_name.startswith('sub-'):  # Process sheets named sub-xx
        df = pd.read_excel(xls, sheet_name=sheet_name, header=0)
        df = df.loc[:, ['sub', 'ses', 'protocol_name','quality_mark']]
        df[['sub', 'ses']] = df[['sub', 'ses']].replace('', pd.NA)
        # Drop rows where both 'sub' and 'ses' are NA
        df = df.dropna(subset=['sub', 'ses'], how='any')
        df['sub'] = df['sub'].astype(int).astype(str).str.zfill(2)  # Convert 'sub' to string
        # filter the df, so that if sub is 11, take it all, if not, will select the ses that greate than 0
        if (df['sub'] == '11').all():
            df=df[~df['ses'].astype(str).str.startswith('ME')]
        else:
            df=df[df['ses'] > 0]
        try:
            # Assuming df is already loaded
            df['ses'] = df['ses'].astype(int).astype(str).str.zfill(2)   # Convert 'ses' to string
        except:
            print(f"the ses col maybe already a str for {sheet_name}")
        # Ensure necessary columns exist
        if all(col in df.columns for col in ['sub', 'ses', 'protocol_name','quality_mark']):
            # 🔹 Filter rows where "protocol_name" contains "rerun" or "skip"
            filtered_rows = df[df['protocol_name'].str.contains('rerun', case=False, na=False) | df['quality_mark'].str.contains('failed', case=False, na=False)]
            filtered_data.append(filtered_rows)

# 🔹 Combine all filtered results into one DataFrame
final_df = pd.concat(filtered_data, ignore_index=True) if filtered_data else pd.DataFrame(
    columns=['sub', 'ses', 'protocol_name','quality_mark'],
)
# Define subject-session pairs to drop
drop_conditions = (
    ((final_df['sub'] == '01') & final_df['ses'].isin(['10', '11']))  # Drop sub-01 ses-10, ses-11
    | ((final_df['sub'] == '05') & final_df['ses'].isin(['03']))          # Drop sub-05 ses-03
)

# Drop rows matching conditions
df_filtered = final_df[~drop_conditions].reset_index(drop=True)

bids_dir = '/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS'
layout = BIDSLayout(bids_dir, validate=False)

for _, row in df_filtered.iterrows():
    sub = str(row['sub'])  # Convert to string and zero-pad
    ses = str(row['ses'])  # Convert to string and zero-pad
    protocol_name = row['protocol_name']

    # Extract run number and rerun number from protocol_name
    parts = protocol_name.split('_')
    run_part = [p for p in parts if 'run-' in p]
    rerun_part = [p for p in parts if 'rerun-' in p or 'rerun' in p]

    if run_part and rerun_part:
        run_number = run_part[0].split('-')[-1]  # e.g., run-11
        rerun_number = rerun_part[0].split('-')[-1]  # e.g., rerun-02

        # Source and target paths
        source_event = layout.get(
            subject=sub, session=ses, run=rerun_number,
            task='fLoc', suffix='events', extension='tsv',
        )[0].path
        target_event = source_event.replace(f'run-{rerun_number}', f'run-{run_number}')
        # Ensure source file exists before linking
        if os.path.exists(source_event):
            # Avoid overwriting existing files
            if not (os.path.exists(target_event) or os.path.islink(target_event)):
                os.symlink(source_event, target_event)
                print(f'Linked {source_event} -> {target_event}')
            else:
                print(f'Target exists, skipping: {target_event}')
        else:
            print(f'Source missing, skipping: {source_event}')
