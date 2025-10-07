# this script is using the pybids to get the 
# nii.gz json and the nii.gz's file name, to see if they match

from __future__ import annotations
import os
import pandas as pd
from bids import BIDSLayout
from launchcontainers.utils import read_df, force_symlink, check_symlink
import warnings
import json



bids_dir = '/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS'
layout = BIDSLayout(bids_dir, validate=False)

subs=layout.get_subject()
task = 'fLoc'

mismatches = []
for sub in subs:
    sess=layout.get_session(subject=sub)
    for ses in sess:
        num_runs= len(layout.get(subject=sub,session=ses,task='fLoc', suffix='bold',extension='nii.gz'))
        run_list=[f'{i:02d}' for i in range(1,num_runs+1)]
        for run in run_list:
            bids_func_nii = layout.get(
                subject=sub, session=ses, run=run,
                task='fLoc', suffix='bold', extension='nii.gz',
            )
            bids_func_json = layout.get(
                subject=sub, session=ses, run=run,
                task='fLoc', suffix='bold', extension='json',
            )
            if not len(bids_func_nii) == 1 or not len(bids_func_json) == 1:
                print(f"number of func nii is not right for S{sub}-T{ses}-{task}-run-{run}")
                mismatches.append({
                    "sub": sub,
                    "ses": ses,
                    "task": task,
                    "bids_run": run,
                    "real_run": None,
                    "note": "Not complete"
                })

                                  
            else:
                func_nii_fpath = bids_func_nii[0].path
                func_json_fpath = bids_func_json[0].path
                # now read the json and get the actual run num from the series description
                with open(func_json_fpath, "r") as f:
                    data = json.load(f)
                real_run_num = [i.strip('run') for i in data['SeriesDescription'].split('_') if i.lower() not in [task.lower()]][0]
                if run != real_run_num:
                    print(f"S{sub}-T{ses}-{task}-run-{run} is not match with the json {real_run_num}")
                    mismatches.append({
                    "sub": sub,
                    "ses": ses,
                    "task": task,
                    "bids_run": run,
                    "real_run": real_run_num[0:2],
                    "note": "mismatch"
                })
                
df_mismatch = pd.DataFrame(mismatches)

error_ses = df_mismatch[['sub','ses']].drop_duplicates()