import os
import pandas as pd
import numpy as np
from datetime import datetime

log_dir = '/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS/log_nordic_dwi/dwi_no_nordic_2026-02-02'
log_files = os.listdir(log_dir)

records = []
for log_file in log_files:
    if not log_file.endswith('.o'):
        continue
    parts = log_file.split("_")
    sub_id = parts[1]
    ses_id = parts[2].split('.')[0]
    sub = f"sub-{sub_id}"
    ses = f"ses-{ses_id}"
    
    with open(os.path.join(log_dir, log_file), 'r') as f:
        content = f.read()
    
    if 'doNORDIC is: 1' in content:
        status = 1
    elif 'doNORDIC is: 0' in content:
        status = 0
    else:
        status = 2
    
    records.append({'sub': sub_id, 'ses': ses_id, 'status': status, 'log_file': log_file})

df = pd.DataFrame(records)
# Deduplicate: keep non-error status over error for same sub-ses
df = (
    df.sort_values('status')  # puts 0/1 before NaN
    .drop_duplicates(subset=['sub', 'ses'], keep='first')
)
# ==============================
# OUTPUT 1: Short summary print
# ==============================
print("=" * 50)
print("ANALYSIS INTEGRITY SUMMARY")
print("=" * 50)
counts = df['status'].value_counts()
for s in [1, 0, 2]:
    print(f"  {s:>10}: {counts.get(s, 0)}")
print(f"  {'TOTAL':>10}: {len(df)}")
print("=" * 50)

# ==============================
# OUTPUT 2: Per sub-ses status list
# ==============================
print("\nDETAILED STATUS:")
print("-" * 50)
for _, row in df.sort_values(['sub', 'ses']).iterrows():
    icon = {1: '✓ NORDIC', 0: '✗ NO NORDIC', 2: '⚠ ERROR'}
    print(f"  {row['sub']}_{row['ses']}: {icon[row['status']]}")

# ==============================
# OUTPUT 3: Pivoted CSV
# ==============================
df['sub'] = 'sub-' + df['sub']
df['ses'] = 'ses-' + df['ses']
pivot = df.pivot_table(index='sub', columns='ses', values='status', aggfunc='first')
pivot = pivot.fillna('2')

csv_path = os.path.join(log_dir, f'nordic_status_pivot_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
pivot.to_csv(csv_path)
print(f"\nPivot CSV saved to: {csv_path}")
print(pivot)