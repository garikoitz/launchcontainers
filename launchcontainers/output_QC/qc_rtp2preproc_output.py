# """
# MIT License
# Copyright (c) 2024-2025 Yongning Lei
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or substantial
# portions of the Software.
# """
from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd


def find_newest_log(log_dir: Path, exts: tuple[str, ...] = ('.err', '.e')) -> Path | None:
    """
    Scan `log_dir` for files matching *_YYYY-MM-DD_HH-MM-SS.<ext>,
    and return the Path to the file with the latest timestamp.
    """
    log_dir = Path(log_dir)
    timestamp_re = re.compile(
        r'.*_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})('
        + '|'.join(re.escape(e) for e in exts)
        + r')$',
    )
    latest = None
    latest_dt = None

    for p in log_dir.iterdir():
        if not p.is_file():
            continue
        m = timestamp_re.match(p.name)
        if not m:
            continue
        ts_str = m.group(1)  # e.g. "2025-04-27_05-08-18"
        ts = datetime.strptime(ts_str, '%Y-%m-%d_%H-%M-%S')
        if latest_dt is None or ts > latest_dt:
            latest_dt, latest = ts, p

    return latest


def check_rtp_preproc_logs(analysis_dir):
    path_to_subse = os.path.join(analysis_dir, 'subseslist.txt')
    df_subSes = pd.read_csv(path_to_subse, sep=',', dtype=str)
    for row in df_subSes.itertuples(index=True, name='Pandas'):
        sub = row.sub
        ses = row.ses
        RUN = row.RUN
        dwi = row.dwi
        if RUN == 'True' and dwi == 'True':
            dwi_file = os.path.join(
                analysis_dir,
                'sub-' + sub,
                'ses-' + ses,
                'output', 'dwi.nii.gz',
            )
            log_file_dir = os.path.join(
                analysis_dir,
                'sub-' + sub,
                'ses-' + ses,
                'log',
            )
            # print(os.listdir(log_file_dir))
            log_file = find_newest_log(log_file_dir)
            if not os.path.isfile(dwi_file):
                print(f'*****for sub-{sub}_ses-{ses}')
                print(f'!!!Issue with sub-{sub}, ses-{ses}*****\n')
            # else:
            #     pass
            #     with open(log_file) as f:
            #         lines = f.readlines()
            #         print(f'*****for sub-{sub}_ses-{ses}')
            #         print(lines[-2] + '###')
            #         if 'Success' in lines[-2].strip():
            #             print(f'No problem with sub-{sub}, ses-{ses}*****\n')


# Example usage:
check_rtp_preproc_logs(
    '/bcbl/home/public/Gari/MINI/paper_dv/BIDS/derivatives/rtppreproc_1.2.0-3.0.3/analysis-paper_dv-retest',
)
