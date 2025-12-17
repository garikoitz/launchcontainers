#!/usr/bin/env python3
"""
DICOM Symlink Organizer
-----------------------
Reorganizes DICOM data by creating symlinks with a standardized structure.

Part of the MRIworkflow project.

Create from src: dcm folder

to targ: project/dicom folder with structure: sub-xx/ses-yy

This script is used to create symlink from project folder to lab folder

First, you need to have your labnote prepared, which will be under /basedir/VOTCLOC_subses_list.xlsx
    the information we get from the labnote is:
    1. sub-xx
    2. ses-yy
    3. date
    4. time_start for each protocol

Feature 1: The script will read the xlsx file and generate a dict which will map the datetime with the session

Feature 2: The script will screening the dcm folder under  ~/lab/MRI/VOTCLOC_22324/DATA/images/ and will
get the datatime and parse the corresponding sub-xx and ses-yy

    This script will take consideration with several cases:
    1. multiple sessions uploaded to the same folder
    2. different level of folder structure
    .... to be continue

Then the script will provide a mapping from lab note to the dcm folder name to match the sessions

The output of this script will be a dataframe to check, after manual check, we can create the symlink

This script will serve as the first step for the dcm to nifti conversion with heudiconv,

Prepare heudiconv

"""

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
def read_lab_note(lab_note_path: Path):
    # 🔹 Path to the downloaded Excel file
    # Replace with your actual file path
    # lab_note_path = '/bcbl/home/public/Gari/VOTCLOC/main_exp/VOTCLOC_subses_list.xlsx'
    # 🔹 Load the Excel file

    xls = pd.ExcelFile(lab_note_path)

    # 🔹 Find all sheets that match "sub-xx"
    all_df = []
    for sheet_name in xls.sheet_names:
        if sheet_name.startswith('sub-'):  # Process sheets named sub-xx
            df = pd.read_excel(xls, sheet_name=sheet_name, header=0)
            print(df['sub'].unique())
            # ✅ Make 'sub' column string type
            df['sub'] = df['sub'].astype(int).astype(str)
            # get the ses is nan sub and ses
            sub = df['sub'].unique()
            if df['ses'].isna().any():
                print(f'sub-{sub} have Nan ses value')
                print(df[df['ses'].isna()])
            df = df.loc[:, ['sub', 'ses', 'date', 'time_start']]
            df = df.dropna()
            # ✅ Convert 'date' to datetime, then format to YYYY-MM-DD
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            all_df.append(df)

    lab_note_df = pd.concat(all_df)

    # apply string to the col
    lab_note_df['time_start'] = lab_note_df['time_start'].astype(str)
    # change the sub format
    lab_note_df['sub'] = lab_note_df['sub'].str.zfill(2)
    # change the ses format
    # first if it is str
    lab_note_df['ses'] = lab_note_df['ses'].apply(
        lambda x: str(x).zfill(2) if isinstance(x, str) and not x.startswith('-') and 't' not in x and len(x) < 2
        or isinstance(x, int)else x,
    )
    # if it is float
    lab_note_df['ses'] = lab_note_df['ses'].apply(
        lambda x: str(int(x)).zfill(2) if isinstance(x, float) else x,
    )

    def parse_time(x):
        for fmt in ('%H:%M:%S', '%H:%M'):
            try:
                return pd.to_datetime(x, format=fmt).time()
            except:
                continue
        return pd.NaT
    # parse the date time
    lab_note_df['time_start'] = lab_note_df['time_start'].apply(parse_time)
    # drop all the nan
    lab_note_df = lab_note_df.dropna()

    # get the latest time for each session
    subses_date = lab_note_df.groupby(['sub', 'ses', 'date'], as_index=False)['time_start'].min()

    # subses_date.to_csv(os.path.join(output_dir,"subses_date_summary.csv"), index=False)
    return subses_date


def guess_subsesid_from_dcm(name: str):
    '''
    Function to parse the dir name and filter it to the BIDS format

    '''
    name = str(name).strip()

    # if it is already sub-xx_ses-xx(_ with some extension) format
    if re.match(r'^sub-\d{2}_ses-(.*)$', name, re.IGNORECASE):
        m = re.search(r'^sub-(\d{2})_ses-(.*)', name, re.IGNORECASE)
    # if it is Sx(Txx(_with some extension)) format
    elif re.search(r'S(\d{1,2})_?T(\d{1,3}(_[A-Za-z0-9]+)?)', name, re.IGNORECASE):
        # ) If it starts with SxxTxx[_suffix]
        m = re.search(r'S(\d{1,2})_?T(\d{1,3}(_?[A-Za-z0-9]+)?)', name, re.IGNORECASE)
    else:
        m = re.search(r'S(\d{2})', name, re.IGNORECASE)
    # now process with the subid and sesid
    sub = f'{int(m.group(1)):02d}'
    try:
        ses_with_info = m.group(2).lower().replace('_', '').replace('-', '')
    except:
        ses_with_info = 'No'
        
    if len(ses_with_info)==1:
        ses_with_info = f"{int(ses_with_info):02d}"
    # Else → keep original
    name = f'sub-{sub}_ses-{ses_with_info}'
    return sub, ses_with_info


def get_ses_suffix(name: str):
    name = str(name).strip()
    # m = re.match(r'sub-\d{2}_ses-\d{2}(.*)', name)
    # if m:
    # one special case for VOTCLOC_22324_S3T402   
    if name =='402':
        ses = '04'
        suffix = '02'
    else:
        ses = name[:2]
        suffix = name[2:]
    if suffix == '':
        label = 'normal'
    elif suffix in ['real', 're', 'july14redo', 'rerun']:
        label = 'rerun'
    elif suffix in ['ME', 'acq-ME', 'acqME']:
        label = 'ME'
    elif suffix in ['SE']:
        label = 'normal'
    else:
        label = 'seperate'
    return ses, suffix , label

@app.command()
def main():
    # # summarize the lab folder
    # lab_project_dir = Path("/export/home/tlei/lab/MRI/VOTCLOC_22324/DATA/images")
    # exclude = ["manual","test", "multisite", "pilot", "ME","check", "Kepa"]
    # output_path = Path('/bcbl/home/public/Gari/VOTCLOC/main_exp/dicom/base_dicom_check_Dec-5.csv')
    # dcm_sum_df = dcm_dir_summary(lab_project_dir,output_path,exclude)
    # # apply set to all the list
    # # dcm_sum_df = dcm_sum_df.applymap(lambda x: set(x) if isinstance(x, tuple) else x)
    # # store the dcm_sum_df because it takes so many time...
    # # do the same thing for the manual subdir and store the output

    # read and merge the 2 dcm summary

    output_dir = '/bcbl/home/public/Gari/VOTCLOC/main_exp/dicom'
    dcm_dir = '/bcbl/home/public/Gari/VOTCLOC/main_exp/dicom'
    dcm_sum_df = pd.read_csv(os.path.join(dcm_dir, 'base_dicom_check_Nov-05.csv'))
    manual_dcm_sum_df = pd.read_csv(os.path.join(dcm_dir, 'manual_dicom_check_Nov-05.csv'))
    dcm_merged = pd.concat([dcm_sum_df, manual_dcm_sum_df], ignore_index=True)
    # first distinguish the origin of dcm either from base of manual
    dcm_merged['origin'] = dcm_merged['lab_project_dir'].apply(
        lambda x: 'manual' if 'manual' in str(x).lower() else 'base',
    )
    # apply the dir name parser to sub and ses
    dcm_merged[['sub','ses_guess']] = dcm_merged['dir_name'].apply(guess_subsesid_from_dcm).apply(pd.Series)
    # sep the ses_guess to ses and note
    dcm_merged[['ses','suffix','note']] = dcm_merged['ses_guess'].apply(get_ses_suffix).apply(pd.Series)
    # expand the acq_date if it is set and make it a list
    dcm_merged['acq_date'] = dcm_merged['acq_date'].str.strip("{}").str.replace("'", '').str.split(',')
    # give a note, if the session have multiple date, then mark it as 
    dcm_merged.loc[ dcm_merged['acq_date'].apply(len)>1,'note'] = 'wrong'
    dcm_merged.loc[ dcm_merged['session_correct']==0,'note'] = 'wrong'
    dcm_expand = dcm_merged.explode('acq_date').reset_index(drop=True)
    dcm_expand['date'] = dcm_expand['acq_date']

    # dcm_expand.loc[(dcm_expand['sub']=='05') & (dcm_expand['ses']=='08'),:]
    # check the ones with suffix
    # dcm_merged[dcm_merged['ses_guess'].apply(lambda x: len(x)>2)]

    # summarize the lab note
    lab_note_path = Path('/bcbl/home/public/Gari/VOTCLOC/main_exp/VOTCLOC_subses_list.xlsx')
    lab_note = read_lab_note(lab_note_path)

    # clean the ses we are not using
    keywords = ['t', 'lost', 'wrong', 'ME', 'failed', 'No', 'bad','-']
    pattern = '|'.join(keywords)
    lab_note = lab_note[~lab_note['ses'].str.contains(pattern, case=False, na=False)]
    # make sure the sub ses and date are all str
    lab_note['sub'] = lab_note['sub'].astype(str).str.zfill(2)
    lab_note['ses'] = lab_note['ses'].astype(str)
    lab_note['date'] = lab_note['date'].astype(str).str.strip()
    
    dcm_expand['sub'] = dcm_expand['sub'].astype(str).str.zfill(2)
    dcm_expand['ses'] = dcm_expand['ses'].astype(str)
    dcm_expand['date'] = dcm_expand['date'].astype(str).str.strip()


    #lab_note.loc[(lab_note['sub']=='05') & (lab_note['ses']=='08'),:]
    # Convert to datetime
    # dcm_expand['datetime'] = pd.to_datetime(dcm_expand['acq_date'] + ' ' + dcm_expand['acq_time'], format='mixed')

    # lab_note['datetime'] = pd.to_datetime(lab_note['date'] + ' ' + lab_note['time_start'].apply(str),format='mixed')

    # # THEN do merge_asof
    # dcm_expand = dcm_expand.sort_values('datetime')
    # lab_note = lab_note.sort_values('datetime')

    # merged = pd.merge_asof(dcm_expand, lab_note, on='datetime',
    #                     tolerance=pd.Timedelta('30min'), 
    #                     direction='nearest',
    #                     suffixes=('_dcm', '_lab'))

    # merge 2 df to get the corresponding correct sub and session for each dcm dir
    merged = pd.merge(
        lab_note,
        dcm_expand, 
        on=['sub','date'], 
        how = 'outer',
        suffixes=('_from_note', '_from_dcm'),
        indicator=True
    )

    # manual edit:
    # sub-03_ses-04 Jan-29 DWI session
 
    merged.sort_values(['sub','ses_from_note']).to_csv('/bcbl/home/public/Gari/VOTCLOC/main_exp/dcm_labnote_summary.csv', index=False)

    #merged.loc[(merged['sub']=='03') & (merged['ses']=='04'),:]

    # start to getting the mapping info
    # only get the both
    merged= merged[merged['_merge']=='both']

    sub_ses_group = merged.groupby(['sub', 'ses_from_note'])
    # create symlink
    force = True
    # after everything, rerun all the dcm conversion
    errors = []
    for sub in range(1, 12):
        for ses in range(1, 11):
            sub = f'{sub:02}'
            ses = f'{ses:02}'
            target_dir = os.path.join(
                output_dir,
                f'sub-{sub}',
                f'ses-{ses}',
            )
            try:
                info = sub_ses_group.get_group((sub, ses))
                # clean the duplicated, drop the manual part 
                info=info.sort_values('origin')
                info=info.drop_duplicates(subset=['sub','ses_from_note','date', 'note','number_of_protocal'], keep = 'first')
                # drop the wrong upload sub-01_ses-01 with autoupload
                idx_to_drop = info.index[
                        (info['number_of_protocal']==26) ]
                info = info.drop(idx_to_drop)
 
                if len(info) > 1 or 'wrong' in info['note'].values:
                    # if there are multiple dcm folder for the same session:
                    if len(info) == 1 and info['note'].item() == 'wrong':
                        errors.append((sub, ses))
                    elif len(info) > 1:
                        idx_to_drop = info.index[
                                (info['note']=='wrong') ]
                        info = info.drop(idx_to_drop)
                        errors.append((sub, ses))
                    
                if len(info) > 1:
                    print(f'sub-{sub}_ses-{ses}')
                    print(info[['number_of_protocal', 'origin','note','dir_name']])

                    # 1 if info is wrong, append it to error
                    if info['note'].item() == 'wrong'
                        errors.append((sub, ses))

                    # 1. auto upload and the manual mixed we drop the manual part and check
                    if len(info['lab_project_dir'].unique()) > 1:
                        idx_to_drop = info.index[
                                (info['number_of_protocal']<50)
                            )
                        ]
                        info = info.drop(idx_to_drop)
                    # after drop the manual and autp upload part
                    if len(info) > 1:
                        print(f'$$need manual correction sub={sub},ses={ses}')
                        print(
                            info[[
                                'sub', 'ses', 'date', 'ses_label',
                                'ses_suffix', 'session_correct',
                            ]],
                        )
                        errors.append((sub, ses))
                    else:
                        if info['session_correct'].item() == 0:
                            print(f'*session have mismatch functional run sub={sub},ses={ses}')
                            errors.append((sub, ses))
                else:
                    # now the series only have 1 item so we can use item to get the value
                    # we can simply create symlink
                    lab_project_dir = info['lab_project_dir'].item()
                    dir_name = info['dir_name'].item()
                    levels_from_top = info['levels_from_top'].item()
                    session_correct = info['session_correct'].item()

                    if session_correct != 0:
                        src_dir = os.path.join(lab_project_dir, dir_name)
                        # print(f'going to create symlink from \n {src_dir} to {target_dir}')
                    else:
                        print(f'*session have mismatch functional run sub={sub},ses={ses}')
                        # force_symlink(src_dir, target_dir,force)
                        errors.append((sub, ses))
                # now we need to parse the dir name to sub -xx ses-xx format
            except:
                print(f'*session have mismatch functional run sub={sub},ses={ses}')
                errors.append((sub, ses))

    print(errors)

# if __name__ == "__main__":
#     app()
