# -----------------------------------------------------------------------------
# Copyright (c) Yongning Lei 2024
# All rights reserved.
#
# This script is distributed under the Apache-2.0 license.
# You may use, distribute, and modify this code under the terms of the Apache-2.0 license.
# See the LICENSE file for details.
#
# THIS SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE, AND NONINFRINGEMENT.
#
# Author: Yongning Lei
# Email: yl4874@nyu.edu
# GitHub: https://github.com/yongninglei
# -----------------------------------------------------------------------------
from __future__ import annotations

import os
import os.path as Path

import pandas as pd
from bids import BIDSLayout


def get_bids_sub_ses(mapping, mini_sub, input_sub_catagory):
    sub = mapping.loc[mapping[input_sub_catagory] == mini_sub, 'BIDS_sub'].values[0]
    ses = mapping.loc[mapping[input_sub_catagory] == mini_sub, 'BIDS_ses'].values[0]

    return sub, ses


def save_onset_to_bids_event(layout, sub, ses, task, run_num, df):
    fname_event = layout.get(
        subject=sub, session=ses, task=task,
        run=f'{run_num:02d}', suffix='events', extension='.tsv',
    )[0].filename
    storepath_event = layout.get(
        subject=sub, session=ses, task=task,
        run=f'{run_num:02d}', suffix='events', extension='.tsv',
    )[0].path
    print(f'saving to {storepath_event}')    # write to csv
    # df.to_csv(Path.join(bidsdir, 'code' ,fname_event), sep='\t')
    df.to_csv(storepath_event, sep='\t', index=False)


# %%
onsetdir = '/bcbl/home/public/Gari/MINI/ANALYSIS/fMRI_SPM/block/data'

basedir = '/bcbl/home/public/Gari/MINI'

bids_dicom_dir = Path.join(basedir, 'dicoms_BLQfunc_T1')


mapping_table = pd.read_csv(
    Path.join(
        basedir, 'dicoms_BLQfunc_T1',
        'ID_mapping.tsv',
    ), sep='\t', header=0,
)

mapping = mapping_table.loc[0:96, ['OsirixID', 'BIDS_sub', 'BIDS_ses', 'MINID']]

input_sub_catagory = 'MINID'

bidsdir = Path.join(basedir, 'BIDS_BLQfunc_T1')

layout = BIDSLayout(bidsdir)

task = 'MINIblock'


# %%
mini_allsubs = [i for i in os.listdir(onsetdir) if i.startswith('S0')]

for i in range(len(mini_allsubs)):

    mini_sub = mini_allsubs[i]
    try:
        sub, ses = get_bids_sub_ses(mapping, mini_sub, input_sub_catagory)

        # %%  read the mini onset.tsv
        spm_onset_fname = Path.join(onsetdir, mini_sub, 'log', f'{mini_sub}.csv')
        spm_onset = pd.read_csv(spm_onset_fname, sep=',', header=0)

        runs = spm_onset.Run
        run_nums = len(runs.value_counts())

        run_group = spm_onset.groupby('Run')
        for i in range(run_nums):
            run_num = i + 1
            run_i = run_group.get_group(run_num)
            bids_run = pd.DataFrame(columns=['onset', 'duration', 'trial_type'])
            duration = run_i['Onset'].diff().shift(-1).fillna(2.42)

            bids_run['onset'] = run_i['Onset']
            bids_run['duration'] = duration
            bids_run['trial_type'] = run_i['Code']
            save_onset_to_bids_event(layout, sub, ses, task, run_num, bids_run)
    except:

        print(f'MINIsub {mini_sub} have no bids sub')
        pass
