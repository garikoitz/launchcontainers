from __future__ import annotations

import os

import pandas as pd


def check_rtp_preproc_logs(analysis_dir):
    path_to_subse = os.path.join(analysis_dir, 'subSesList.txt')
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
                'output', 'dwi.nii.gz'
            )
            log_file_dir=os.path.join(
                analysis_dir,
                'sub-' + sub,
                'ses-' + ses,
                'output', 'log'
            )

            log_file=os.path.join(log_file_dir, 
            [f for f in os.listdir(log_file_dir) if f.endswith('.err')][0])
            if not os.path.isfile(dwi_file):
                print(f'*****for sub-{sub}_ses-{ses}')
                print(f'!!!Issue with sub-{sub}, ses-{ses}*****\n')
            else:
                with open(log_file) as f:
                    lines = f.readlines()
                    print(f'*****for sub-{sub}_ses-{ses}')
                    print(lines[-2] + '###')
                    if 'Success' in lines[-2].strip():
                        print(f'No problem with sub-{sub}, ses-{ses}*****\n')
                


# Example usage:
check_rtp_preproc_logs(
    '/scratch/tlei/paper_dv/BIDS/derivatives/rtppreproc_1.2.0-3.0.3/analysis-paper_dv-retest_newlaunch',
)
