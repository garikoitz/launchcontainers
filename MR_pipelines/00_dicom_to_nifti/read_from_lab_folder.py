from __future__ import annotations

import glob
import os
import re
from datetime import datetime

import pandas as pd
# Example input list (replace with your full list)
raw_sessions = pd.read_csv(
    '/bcbl/home/public/Gari/VOTCLOC/main_exp/dicom/images_in_lab_aug_12.txt',
    header=None, names=['ses_raw'],
)


def process_session(name):
    name = str(name).strip()

    # If it's already sub-xx_ses-xx or sub-xx_ses-xx_suffix → keep as is
    if re.match(r'^sub-\d{2}_ses-(.*)$', name, re.IGNORECASE):
        return name.lower()

    # If it starts with SxxTxx[_suffix]
    m = re.match(r'^S(\d{1,2})_?T(\d{1,2}(_[A-Za-z0-9]+)?)', name, re.IGNORECASE)
    if m:
        return f'sub-{int(m.group(1)):02d}_ses-{m.group(2).lower()}'

    # If it starts with VOTCLOC_22324 / VOTCLOC / votcloc and has SxxTxx[_suffix]
    if re.match(r'^(VOTCLOC_22324|VOTCLOC|votcloc)_', name):
        m = re.search(r'S(\d{1,2})_?T(\d{1,2}(_[A-Za-z0-9]+)?)', name, re.IGNORECASE)
        if m:
            return f'sub-{int(m.group(1)):02d}_ses-{m.group(2).lower()}'

    # Else → keep original
    return name


# Apply function
raw_sessions['processed'] = raw_sessions['ses_raw'].apply(process_session)
# then I am going to read from the lab folder to get each ses a date

lab_dir = '/export/home/tlei/lab/MRI/VOTCLOC_22324/DATA/images'


# df has a column 'raw' with entries like:
# "S1T06/AAHead_Scout_64ch-head-coil_1/1.3.12.2.1107.5.2.43.167004.2025020413590579004684134.dcm"
# Set your base directory:

def find_aahead_scout_dir(base, raw):
    """Return the AAHead_Scout* directory path under base/raw, else None."""
    root = os.path.join(base, raw)
    if not os.path.isdir(root):
        return None
    try:
        # match any folder starting with AAHead_Scout (case-insensitive)
        candidates = [
            d for d in os.listdir(root)
            if os.path.isdir(os.path.join(root, d)) and d.lower().startswith('aahead_scout')
        ]
    except FileNotFoundError:
        return None
    if not candidates:
        return None
    # choose the first in sorted order for determinism
    return os.path.join(root, sorted(candidates)[0])


def pick_dcm_file(folder):
    """Return a .dcm file path under folder (recursively), else None."""
    if not folder or not os.path.isdir(folder):
        return None
    files = glob.glob(os.path.join(folder, '**', '*.dcm'), recursive=True)
    return sorted(files)[0] if files else None


def parse_date_from_filename(path):
    """
    From a DICOM filename, extract an 8-digit date (YYYYMMDD).
    If present, optionally also extract following 6-digit time (HHMMSS).
    Returns 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS' if time found; else None.
    """
    if not path:
        return None
    fname = os.path.basename(path)
    # Find an 8-digit date optionally followed by 6-digit time
    m = re.search(r'(?P<date>\d{8})(?P<time>\d{6})?', fname)
    if not m:
        return None
    ymd = m.group('date')
    t = m.group('time')
    try:
        if t:
            dt = datetime.strptime(ymd + t, '%Y%m%d%H%M%S')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            d = datetime.strptime(ymd, '%Y%m%d').date()
            return d.strftime('%Y-%m-%d')
    except ValueError:
        return None


def extract_acq_date(raw):
    scout_dir = find_aahead_scout_dir(lab_dir, raw)
    dcm = pick_dcm_file(scout_dir)
    return parse_date_from_filename(dcm)


# Apply to the DataFrame
raw_sessions['date'] = raw_sessions['ses_raw'].apply(extract_acq_date)


votcloc_sessions = raw_sessions[raw_sessions['processed'].str.startswith('sub')]

votcloc_sessions['sub'] = votcloc_sessions['processed'].apply(
    lambda x: x.split('_', 1)[0].removeprefix('sub-'),
)
votcloc_sessions['ses'] = votcloc_sessions['processed'].apply(
    lambda x: x.split('_', 1)[1].removeprefix('ses-'),
)

sub_group = votcloc_sessions.groupby('sub')

sub01 = sub_group.get_group('01')
sub02 = sub_group.get_group('02')
sub03 = sub_group.get_group('03')
sub04 = sub_group.get_group('04')
sub05 = sub_group.get_group('05')
sub06 = sub_group.get_group('06')
sub07 = sub_group.get_group('07')
sub08 = sub_group.get_group('08')
sub09 = sub_group.get_group('09')
sub10 = sub_group.get_group('10')
sub11 = sub_group.get_group('11')

output_date_time_dicom = '/bcbl/home/public/Gari/VOTCLOC/main_exp/dicom'
for sub in [f'{i:02d}' for i in range(1, 11)]:
    output_fname = f'sub-{sub}_dicom_date_and_dirname.txt'
    print(output_fname)
    sub_group.get_group(sub).to_csv(
        os.path.join(
            output_date_time_dicom, output_fname,
        ), index=False, sep=',',
    )
