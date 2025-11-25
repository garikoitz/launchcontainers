from __future__ import annotations
import os
import pandas as pd
from bids import BIDSLayout
from launchcontainers.utils import read_df, force_symlink, check_symlink

### excecution
if __name__ == "__main__":
    # 🔹 Path to the downloaded Excel file
    # Replace with your actual file path
    xlsx_file = '/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS/sourcedata/VOTCLOC_subses_list.xlsx'
    # 🔹 Load the Excel file
    xls = pd.ExcelFile(xlsx_file)

    # 🔹 Find all sheets that match "sub-xx"
    filtered_data = []
    all_subses = []
    for sheet_name in xls.sheet_names:
        if sheet_name.startswith('sub-'):  # Process sheets named sub-xx
            df = pd.read_excel(xls, sheet_name=sheet_name, header=0)
            df = df.loc[:, ['sub', 'ses', 'date','protocol_name','quality_mark']]
            df[['sub', 'ses']] = df[['sub', 'ses']].replace('', pd.NA)

            # Drop rows where both 'sub' and 'ses' are NA
            df = df.dropna(subset=['sub', 'ses'], how='any')
            df['sub'] = df['sub'].astype(int).astype(str).str.zfill(2)  # Convert 'sub' to string

            bad_patterns = r"-|wrong|failed|lost|ME|bad|00|test|-t"
            df = df[~df['ses'].astype(str).str.contains(bad_patterns, na=False)]
            try:
                if not pd.api.types.is_string_dtype(df['ses']):
                    df['ses'] = df['ses'].astype(int).astype(str).str.zfill(2)
            except:
                print(f"the ses col maybe already a str for {sheet_name}")
            # Ensure necessary columns exist
            if all(col in df.columns for col in ['sub', 'ses','date','protocol_name','quality_mark']):
                # 🔹 Filter rows where "protocol_name" contains "rerun" or "skip"
                filtered_rows = df[df['protocol_name'].str.contains('rerun', case=False, na=False) & ~df['protocol_name'].str.contains(r'qmri|T1', case=False, na=False)] # | df['quality_mark'].str.contains('failed', case=False, na=False)]
                filtered_data.append(filtered_rows)
            all_subses.append(df)

    # 🔹 Combine all filtered results into one DataFrame
    subses_with_rerun = pd.concat(filtered_data, ignore_index=True) if filtered_data else pd.DataFrame(
        columns=['sub', 'ses', 'protocol_name','quality_mark'],
    )

    # subses_schedule
    # subses_schedule = pd.concat(all_subses, ignore_index=True).loc[:, ['sub', 'ses', 'date']].drop_duplicates().reset_index(drop=True)
    # subses_schedule['date']=pd.to_datetime(subses_schedule['date'],errors='coerce')
    # subses_schedule.to_csv("/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS/sourcedata/subses_schedule.csv", index=False)
    
    # no need to drop anything, now the data is finished
    # Drop rows matching conditions
    df_filtered = subses_with_rerun

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
        task_part = [p for p in parts if "fLoc" in p]
        if run_part and rerun_part and task_part:
            run_number = run_part[0].split('-')[-1]  # e.g., run-11
            rerun_number = rerun_part[0].split('-')[-1]  # e.g., rerun-02

            # Source and target paths
            try:
                source_event = layout.get(
                    subject=sub, session=ses, run=rerun_number,
                    task='fLoc', suffix='events', extension='tsv',
                )[0].path
                target_event = source_event.replace(f'run-{rerun_number}', f'run-{run_number}')
                # Ensure source file exists before linking
                if os.path.exists(source_event):
                    # Avoid overwriting existing files
                    if not (os.path.exists(target_event) or os.path.islink(target_event)):
                        force_symlink(source_event, target_event,True)
                        print(f'Linked {source_event} -> {target_event}')
                    else:
                        print(f'Target exists, skipping: {target_event}')
                else:
                    print(f'Source missing, skipping: {source_event}')
            except Exception as e:
                print(f"error for {sub}, {ses} because of {e}")


## below, add a check to see if the run num of tsv is matching with run num of func
subs=layout.get_subject()
for sub in subs:
    sess=layout.get_session(subject=sub)
    for ses in sess:
        bids_func=layout.get(
            subject=sub,
            session=ses,
            datatype='func',task='fLoc',suffix='bold',extension='nii.gz',
            return_type='list')
       