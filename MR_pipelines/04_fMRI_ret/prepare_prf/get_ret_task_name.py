import numpy as np
import pandas as pd
from scipy.io import loadmat
from glob import glob
from os import path
from collections import defaultdict
import typer
from pathlib import Path
from bids import BIDSLayout

def get_ret_taskname(sourcedata: str, subjects: list, sessions: list):
    """
    Extract all stimnames and categorize them into RW, FF, CB columns.

    Returns a DataFrame with columns:
        sub | ses | RW | FF | CB
    where each cell contains a list (possibly empty).
    """

    # container: (sub, ses) → dict with keys RW/FF/CB
    task_category = defaultdict(lambda: {"RW": [], "FF": [], "CB": []})

    for sub in subjects:
        for ses in sessions:

            sub_str = str(sub).zfill(2)
            ses_str = str(ses).zfill(2)

            mat_files = np.sort(
                glob(path.join(
                    sourcedata,
                    f"sub-{sub_str}",
                    f"ses-{ses_str}",
                    "20*.mat"
                ))
            )

            if mat_files.size == 0:
                print(f"[WARNING] No .mat files for sub-{sub_str} ses-{ses_str}")
                continue

            for mf in mat_files:
                try:
                    mat = loadmat(mf, simplify_cells=True)
                    stimName = mat["params"]["loadMatrix"]
                except Exception as e:
                    print(f"[ERROR] Failed reading {mf}: {e}")
                    continue

                # ------ Categorize stimName ------
                stim = stimName.lower()

                # RW category
                if "rw_" in stim or "fixrw_" in stim:
                    task_category[(sub_str, ses_str)]["RW"].append(stimName.split('/')[-1].split('_')[1])

                # FF category
                elif "ff_" in stim or "fixff_" in stim:
                    task_category[(sub_str, ses_str)]["FF"].append(stimName.split('/')[-1].split('_')[1])

                # CB category (fixRWblock*)
                elif "cb_" or "fixrwblock" in stim:
                    task_category[(sub_str, ses_str)]["CB"].append(stimName.split('/')[-1].split('_')[1])

                else:
                    print(f"[INFO] Unclassified stimName {stimName.split('/')[-1].split('_')[1]} (sub-{sub_str} ses-{ses_str})")

    # Convert dict → DataFrame
    rows = []
    for (sub_str, ses_str), d in task_category.items():
        rows.append({
            "sub": sub_str,
            "ses": ses_str,
            "RW": d["RW"],
            "FF": d["FF"],
            "CB": d["CB"],
        })

    df = pd.DataFrame(rows)
    df = df.sort_values(["sub", "ses"]).reset_index(drop=True)
    return df

def check_params_and_bids(layout, fp_layout, sourcedata, sub, ses):
    # need to check if params and bids task name match,
    # they might not match because I put fixRWblock as CB

    # 1. get all the tasks from BIDS fmriprep and sourcedata
    tasks_in_bids =[i for i in layout.get_tasks(subject= sub, session=ses) if i not in ['fLoc']]
    
    tasks_in_fmriprep =[i for i in fp_layout.get_tasks(subject= sub, session=ses) if i not in ['fLoc']]
    matFiles = np.sort(
        glob(
            path.join(
                sourcedata,
                f'sub-{sub}', f'ses-{ses}', '20*.mat',
            ),
        ),
    )
    if matFiles.size != 0 :
        pass
    else:
        print(f'##### sub-{sub} ses-{ses} Not get the matfiles, please check path')
    tasks_in_sourcedata=[]
    for matFile in matFiles:

        stimName = loadmat(matFile, simplify_cells=True)['params']['loadMatrix'].split('/')[-1].split('_')[1]
        tasks_in_sourcedata.append(f'ret{stimName}')

    # 2. check, if the tasks are in fMRIprep
    tasks_in_bids = set(tasks_in_bids)
    tasks_in_fmriprep = set(tasks_in_fmriprep)
    tasks_in_sourcedata = set(tasks_in_sourcedata)

    if tasks_in_bids == tasks_in_fmriprep:
        pass
    else:
        print(f'FMRIPREP error sub-{sub}_ses-{ses}')
    
    # 3. check, if the task are in sourcedata
    if tasks_in_bids == tasks_in_sourcedata:
        pass
    else:
        print(f'sourcedata error sub-{sub}_ses-{ses}')

    return
if __name__ == '__main__':
    sourcedata = Path('/scratch/tlei/VOTCLOC/BIDS/sourcedata')
    subjects = list(range(1,12))
    sessions = list(range(1,11))
    df = get_ret_taskname(sourcedata, subjects, sessions)
    print(df)
    output_path = Path('/scratch/tlei/VOTCLOC/ret_task_naming.csv')
    df.to_csv(output_path, sep=',', index=False)

    basedir = Path('/scratch/tlei/VOTCLOC')
    bidsdir= basedir / 'BIDS'
    fpdir = basedir / 'BIDS' / 'derivatives' / 'fmriprep-25.1.4_t2-fs_dummyscans-5_bold2anat-t2w_forcebbr'

    bidslo = BIDSLayout(bidsdir, validate = False)
    fplo = BIDSLayout(fpdir, validate = False)

    for sub in subjects:
        for ses in sessions:

            sub = str(sub).zfill(2)
            ses = str(ses).zfill(2)
            check_params_and_bids(bidslo, fplo, sourcedata, sub, ses)
    # subjects = bidslo.get_subjects()
    # for sub in subjects:
    #     sessions = bidslo.get_sessions(subject = sub)
    #     for ses in sessions:
'''
sourcedata error sub-01_ses-08
FMRIPREP error sub-02_ses-09
sourcedata error sub-02_ses-09
FMRIPREP error sub-02_ses-10
sourcedata error sub-02_ses-10
sourcedata error sub-03_ses-08
sourcedata error sub-04_ses-01
sourcedata error sub-05_ses-07
sourcedata error sub-05_ses-08
FMRIPREP error sub-05_ses-10
FMRIPREP error sub-06_ses-01
FMRIPREP error sub-06_ses-02
FMRIPREP error sub-06_ses-03
FMRIPREP error sub-06_ses-04
FMRIPREP error sub-06_ses-05
FMRIPREP error sub-06_ses-06
FMRIPREP error sub-06_ses-07
FMRIPREP error sub-06_ses-08
FMRIPREP error sub-06_ses-09
sourcedata error sub-06_ses-09
sourcedata error sub-07_ses-10
sourcedata error sub-08_ses-04
FMRIPREP error sub-09_ses-10
sourcedata error sub-10_ses-02
sourcedata error sub-10_ses-03
sourcedata error sub-10_ses-04
sourcedata error sub-10_ses-06
FMRIPREP error sub-10_ses-10
FMRIPREP error sub-11_ses-02
FMRIPREP error sub-11_ses-03
sourcedata error sub-11_ses-04
sourcedata error sub-11_ses-05
FMRIPREP error sub-11_ses-06
FMRIPREP error sub-11_ses-07
FMRIPREP error sub-11_ses-08
FMRIPREP error sub-11_ses-09
sourcedata error sub-11_ses-10
'''