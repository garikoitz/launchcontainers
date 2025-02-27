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
import os
import os.path as path
import shutil
from bids import BIDSLayout
import pandas as pd

"""
This code should be able to create symbolic link between sub- /ses- /func/xx.events.tsv to the BIDS/sourcedata/fMRI_log/

"""

basedir='/bcbl/home/public/Gari/VOTCLOC/main_exp'
onset_dir=f'{basedir}/BIDS/sourcedata/fMRI_log'
output_dir=f'{basedir}/BIDS'


#%%
sub_list=['04'] 
ses_list=['02','03','04'] 
runs=10
task="fLoc"

def get_onset_onset_dirname(onset_dir,sub,ses):
    votcloc_logs=[i for i in os.listdir(onset_dir) if '1back' in i]
    for i in votcloc_logs:
        print(i)
        dirname=i
        print(dirname)
        subses=f'sub-{sub}_ses-{ses}'
        print(subses)
        if subses in dirname:
            onset_dirname= dirname
        else:
            onset_dirname=None
    print(f"Onset_dirname_we got is {onset_dirname}")
    return onset_dirname

for i in sub_list:
    for j in ses_list:
        print(f"sub is {i}, ses is {j}")

        onset_dirname=get_onset_onset_dirname(onset_dir,i,j)
        all_onsets=os.path.join(onset_dir,onset_dirname)
        for r in range(runs):
            run_num=f'{(r+1):02}'
            # get the events.tsv filename
            src_fname=f'sub-{i}_ses-{j}_task-{task}_run-{run_num}_events.tsv'
            target_fname=f'sub-{i}_ses-{j}_task-{task}_run-{run_num}_events.tsv'
            src_onset=path.join(all_onsets,src_fname)
            target_path=path.join(output_dir,f'sub-{i}',f'ses-{j}','func')
            
            print(f'src file exist: {path.exists(src_onset)} \n and dst path exists: {path.exists(target_path)}')
            
            if not path.exists(target_path):
                os.makedirs(target_path)
            
            target=path.join(output_dir,f'sub-{i}',f'ses-{j}','func',target_fname)
            if os.path.exists(target):
                os.remove(target)
            try:
                os.symlink(src_onset,target)
                print(f'symlink create copied to {target}')
            except:
                continue

