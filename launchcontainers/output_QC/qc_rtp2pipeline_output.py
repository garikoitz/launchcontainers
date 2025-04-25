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

import pandas as pd


def check_rtp_logs(analysis_dir):
    path_to_subse = os.path.join(analysis_dir, 'subSesList.txt')
    df_subSes = pd.read_csv(path_to_subse, sep=',', dtype=str)
    for row in df_subSes.itertuples(index=True, name='Pandas'):
        sub = row.sub
        ses = row.ses
        RUN = row.RUN
        dwi = row.dwi
        if RUN == 'True' and dwi == 'True':
            log_file = os.path.join(
                analysis_dir,
                'sub-' + sub,
                'ses-' + ses,
                'output', 'log', 'RTP_log.txt',
            )

            if os.path.isfile(log_file):
                with open(log_file) as f:
                    lines = f.readlines()
                    print(f'*****for sub-{sub}_ses-{ses}')
                    print(lines[-1] + '###')
                    if lines[-1].strip() != 'Sending exit(0) signal.':
                        print(f'!!!Issue with sub-{sub}, ses-{ses}*****\n')
            else:
                print(f'Log file missing for sub-{sub}, ses-{ses}')


# Example usage:
check_rtp_logs(
    '/bcbl/home/public/DB/devtrajtract/DATA/MINI/nifti/derivatives/rtp2-pipeline_0.2.1_3.0.4rc2/analysis-dipc_paper_dv-60subj',
)
