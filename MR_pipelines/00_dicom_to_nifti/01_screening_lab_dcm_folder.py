import os
import re
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pydicom
import typer

app = typer.Typer()


@app.command()
def dcm_dir_summary(
    lab_project_dir: Path,
    output_dir: Path,
    exclude: list[str] = typer.Option(default=[])
    
):
    '''
    This script is used to walk the dcm session folder and to get:
    1. the depth

    2. the datetime info within this session

    It will output a dict mapping those info

    If exclude is passed, we will ignore the session folder

    '''
    rows = []

    # Get only first-level subdir_names under lab_project_dir
    session_dirs = [
        d for d in os.listdir(lab_project_dir)
        if os.path.isdir(os.path.join(lab_project_dir, d))
    ]
    # get all the dir under the basedir, for every sesdir inside it we will walk the dir
    # the walk dir looping is at the subdir level (i.e., there are 10 subdirs, it will do 10 iter)
    for ses_dir_name in session_dirs:
        ses_dir_path = lab_project_dir / ses_dir_name
        print(f'Working on {ses_dir_path}')
        # get the number of subdirs under images/xx/
        # there is no qmri in the folder, will count it as 1
        protocal_count = 1
        functional_protocols = 0
        dwi_protocols = 0
        # get if the session is having correct number of functional dcms
        ses_correct = 1
        # this needs to be a set
        acq_dates = []
        # don't read any dir name with manual test multisite and pilot
        if not any(x in ses_dir_name.lower() for x in exclude):
            for dirpath, subdir_names, file_names in os.walk(ses_dir_path):
                # if there is no more subdir_names under the path and there are file_names
                if not subdir_names and file_names:
                    # get the depth for heudiconv
                    rel_from_top = os.path.relpath(dirpath, ses_dir_path)
                    if rel_from_top == '.':
                        depth = 1   # file directly in the top-level dir
                    else:
                        depth = len(rel_from_top.split(os.sep)) + 1
                    protocal_count += 1
                    if 'floc' in dirpath.lower() or 'ret' in dirpath.lower():
                        functional_protocols += 1
                    if 'dmri' in dirpath.lower():
                        dwi_protocols += 1
                    if 'Phoenix' in dirpath:
                        protocal_count -= 1
                    # filter if the dcm transfer is correct
                    if len(file_names) > 209:
                        print(
                            f'WARNING !!! the number of file_names of this ses is not correct {ses_dir_name}',
                        )
                        ses_correct = 0
                    # get the acq date
                    if 'Phoenix' not in dirpath:
                        dcm = pydicom.dcmread(os.path.join(dirpath, file_names[0]))
                        fmt = '%Y%m%d%H%M%S.%f'
                        dt = datetime.strptime(dcm.AcquisitionDateTime, fmt)
                        acq_date = dt.strftime('%Y-%m-%d')
                        acq_time = dt.strftime('%H:%M:%S')
                        # print(f'acquition date if {acq_date}')
                        acq_dates.append(acq_date)
            # convert the acq_date to a set
            if len(set(acq_dates)) > 1:
                print(f'WARNING different date time in one dir, error {ses_dir_name}')
                func_corr = 0
                acq_date = set(acq_dates)
            protocal_count = protocal_count - functional_protocols + functional_protocols/4 - dwi_protocols + dwi_protocols/2
            rows.append({
                'lab_project_dir': lab_project_dir,
                'dir_name': ses_dir_name,
                'levels_from_top': depth,
                'number_of_protocal': protocal_count,
                'numer_of_func': functional_protocols/4,
                'example_file_name': file_names[0],
                'session_correct': ses_correct,
                'acq_date': acq_date,
                'acq_time': acq_time,
            })

    dcm_sum_df = pd.DataFrame(
        rows, columns=[
            'lab_project_dir',
            'dir_name',
            'levels_from_top',
            'number_of_protocal',
            'numer_of_func',
            'example_file_name',
            'session_correct',
            'acq_date',
            'acq_time',
        ],
    )
    dcm_sum_df.to_csv(output_dir, index=False)
    return dcm_sum_df

@app.command()
def main():
    # summarize the lab folder
    lab_project_dir = Path("/export/home/tlei/lab/MRI/VOTCLOC_22324/DATA/images")
    exclude = ["manual","test", "multisite", "pilot", "ME","check", "Kepa"]
    output_path = Path('/bcbl/home/public/Gari/VOTCLOC/main_exp/dicom/base_dicom_check_Dec-5.csv')
    dcm_sum_df = dcm_dir_summary(lab_project_dir,output_path,exclude)

# if __name__ == "__main__":
#     app()
