# """
# MIT License
# Copyright (c) 2022-2025 Yongning Lei
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to permit persons to
# whom the Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.
# """
from __future__ import annotations

import os

import pandas as pd
import typer


def generate_tract_path_list(analysis_dir, tract_prefix, df_subses, output_dir):
    tract = f'MNI_{tract_prefix}_clean_fa_bin.nii.gz'
    print(tract)
    paths = []
    missing = []
    for row in df_subses.itertuples(index=True):
        sub = row.sub
        ses = row.ses
        RUN = row.RUN
        if str(RUN).lower() == 'true':
            tract_fpath = os.path.join(
                analysis_dir,
                f'sub-{sub}',
                f'ses-{ses}',
                'MNI_tract',
                tract,
            )
            # print(tract_fpath)
            if os.path.exists(tract_fpath):
                paths.append(tract_fpath)
            else:
                print(f'missing : sub-{sub} ses-{ses}')
                missing.append({'sub': sub, 'ses': ses})


def find_subseslist(analysis_dir):
    for dirpath, dirnames, filenames in os.walk(analysis_dir):
        for fname in filenames:
            if fname.lower() == 'subseslist.txt':
                return os.path.join(dirpath, fname)
    raise FileNotFoundError(f'No subseslist.txt found under {analysis_dir}')


def check_rtp2_pipeline_logs(analysis_dir):
    path_to_subses = find_subseslist(analysis_dir)
    df_subSes = pd.read_csv(path_to_subses, sep=',', dtype=str)
    for row in df_subSes.itertuples(index=True, name='Pandas'):
        sub = row.sub
        ses = row.ses
        RUN = row.RUN
        dwi = row.dwi
        if RUN == 'True' and dwi == 'True':
            log_file = os.path.join(
                analysis_dir,
                f'sub-{sub}',
                f'ses-{ses}',
                'output', 'log', 'RTP_log.txt',
            )

            if os.path.isfile(log_file):
                with open(log_file) as f:
                    lines = f.readlines()
                    # print(f'*****for sub-{sub}_ses-{ses}')
                    # print(lines[-1] + '###')
                    if lines[-1].strip() != 'Sending exit(0) signal.':
                        print(f'!!!Issue with sub-{sub}, ses-{ses}*****\n')
            else:
                print(f'Log file missing for sub-{sub}, ses-{ses}')


if __name__ == '__main__':
    typer.run(check_rtp2_pipeline_logs)
