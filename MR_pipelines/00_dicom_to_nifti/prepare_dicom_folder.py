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
'''
This is used to prepare the dicom folder if you have a table that stores the IsitixID and BIDs id
'''
from __future__ import annotations

import os
import shutil
from os import makedirs
from os import path

import pandas as pd

basedir = '/bcbl/home/public/Gari/MINI/paper_dv'
code_dpath = path.join(basedir, 'code')
csv_fpath = path.join(code_dpath, 'MINI_SEGU.csv')

# getting things from OsirixID, if the file name matches,
# create folder structure from
# BIDS id

# first load the csv
ref_csv = pd.read_csv(csv_fpath, sep=',', header=0)


def get_bids_subses(ref_df, input_id, input_col_name='OsirixID'):
    row = ref_df[ref_df[input_col_name] == input_id]
    if not row.empty:
        return row[['BIDS_sub', 'BIDS_ses']].iloc[0].to_dict()
    else:
        return None


dicom_dir = path.join(basedir, 'dicom')
input_id_list = [i for i in os.listdir(dicom_dir) if 'DAY2' in i]


def create_dicom_struct(dicom_dir, ref_df, dcm_ses_name):
    # get the sub and ses from ref df
    sub = get_bids_subses(ref_df, dcm_ses_name)['BIDS_sub']
    ses = get_bids_subses(ref_df, dcm_ses_name)['BIDS_ses']

    subses_dir = path.join(dicom_dir, f'sub-{sub}', f'ses-{ses}')
    orig_dcm_ses_dir = path.join(dicom_dir, dcm_ses_name)
    # create the sub ses dir
    makedirs(subses_dir, exist_ok=True)
    # define the output dir
    output = path.join(subses_dir, dcm_ses_name)
    # move if the dest are not there
    if not path.exists(output):
        # move the corresbonding folder to the subses dir
        shutil.move(orig_dcm_ses_dir, subses_dir)

    print(f'for {sub}, {ses}, output is here: {path.exists(output)}')


# the main part:
for dcm_ses_name in input_id_list:
    create_dicom_struct(dicom_dir, ref_csv, dcm_ses_name)
