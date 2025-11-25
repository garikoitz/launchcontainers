'''
This script is used to create symlink from project folder to lab folder

First, you need to have your labnote prepared, which will be under /basedir/VOTCLOC_subses_list.xlsx

Then, the script will read the xlsx file and generate a dict which will map the datetime with the session

Then, the script will do a dicom meta data read from the lab MRI and generate a dict which will map
the datetime with the ~/lab/MRI/VOTCLOC_22324/DATA/images/ folders

In the meantime the script will take consideration with several cases:
1. multiple sessions uploaded to the same folder
2. different level of folder structure
.... to be continue

'''
from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import pydicom
import typer

app = typer.Typer()


@app.command()
def dcm_dir_summary(
    lab_project_dir: Path,
    exclude: list[str] = typer.Option(default=[]),
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
    # get all the dir under the basedir, for every subdir inside it we will walk the dir
    # the walk dir looping is at the subdir level (i.e., there are 10 subdirs, it will do 10 iter)
    for ses_dir_name in session_dirs:
        ses_dir_path = lab_project_dir / ses_dir_name
        print(f'Working on {ses_dir_path}')
        # get the number of subdirs under images/xx/
        protocal_count = 0
        # get if the session is having correct number of functional dcms
        func_correct = 1
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
                    # filter if the dcm transfer is correct
                    if len(file_names) > 209:
                        print(
                            f'WARNING !!! the number of file_names of this ses is not correct {ses_dir_name}',
                        )
                        func_correct = 0
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

            rows.append({
                'lab_project_dir': lab_project_dir,
                'dir_name': ses_dir_name,
                'levels_from_top': depth,
                'number_of_protocal': protocal_count,
                'example_file_name': file_names[0],
                'session_correct': func_correct,
                'acq_date': acq_date,
                'acq_time': acq_time,
            })

    output_dir = '/bcbl/home/public/Gari/VOTCLOC/main_exp/dicom'
    dcm_sum_df = pd.DataFrame(
        rows, columns=[
            'lab_project_dir',
            'dir_name',
            'levels_from_top',
            'number_of_protocal',
            'example_file_name',
            'session_correct',
            'acq_date',
            'acq_time',
        ],
    )
    dcm_sum_df.to_csv(os.path.join(output_dir, 'base_dicom_check_oct-29.csv'), index=False)
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


def process_session(name: str):
    '''
    Function to parse the dir name and filter it to the BIDS format

    '''
    name = str(name).strip()

    # if it is already sub-xx_ses-xx(_ with some extension) format
    if re.match(r'^sub-\d{2}_ses-(.*)$', name, re.IGNORECASE):
        m = re.search(r'^sub-(\d{2})_ses-(.*)', name, re.IGNORECASE)
    # if it is Sx(Txx(_with some extension)) format
    else:
        # ) If it starts with SxxTxx[_suffix]
        m = re.search(r'S(\d{1,2})_?T?(\d{1,3}(_[A-Za-z0-9]+)?)', name, re.IGNORECASE)
    # now process with the subid and sesid

    sub = f'{int(m.group(1)):02d}'

    ses_info = m.group(2)

    if ses_info is None:
        # there is no sesinnfo
        ses = 'No'
    elif len(ses_info) <= 2:
        # there are suffix and moreinfo
        ses = f'{int(ses_info):02d}'
    elif len(ses_info) > 2:
        m2 = re.match(r'(\d+)_?-?(.*)', ses_info)
        if m2:
            ses, suffix = m2.groups()
        else:
            ses, suffix = 'No', None
        # now give the ses with the suffix
        if len(ses) > 2:
            ses = f'{int(ses[:-2]):02d}{suffix}'
        else:
            ses = f'{int(ses):02d}{suffix}'

    # Else → keep original
    name = f'sub-{sub}_ses-{ses}'
    return name


def get_ses_suffix(name: str):
    name = str(name).strip()
    m = re.match(r'sub-\d{2}_ses-\d{2}(.*)', name)
    if m:
        suffix = m.group(1)
    if suffix == '':
        label = 'normal'
    elif suffix in ['real', 're', 'july14redo', 'rerun']:
        label = 'rerun'
    elif suffix in ['ME', 'acq-ME']:
        label = 'ME'
    elif suffix in ['SE']:
        label = 'normal'
    else:
        label = 'seperate'
    return suffix , label


@app.command()
def main():
    # # summarize the lab folder
    # lab_project_dir = Path("/export/home/tlei/lab/MRI/VOTCLOC_22324/DATA/images")
    # exclude = ("manual","test", "multisite", "pilot", "ME","check")
    # dcm_sum_df2 = dcm_dir_summary(lab_project_dir,exclude)
    # # apply set to all the list
    # # dcm_sum_df = dcm_sum_df.applymap(lambda x: set(x) if isinstance(x, tuple) else x)
    # # store the dcm_sum_df because it takes so many time...
    output_dir = '/bcbl/home/public/Gari/VOTCLOC/main_exp/dicom'
    # dcm_sum_df.to_csv(os.path.join(output_dir,"base_dicom_check_oct-29.csv"), index=False)
    dcm_sum_df = pd.read_csv(os.path.join(output_dir, 'base_dicom_check_Nov-05.csv'))
    # # do the same thing for the manual subdir and store the output
    # manual_lab_project_dir = Path("/export/home/tlei/lab/MRI/VOTCLOC_22324/DATA/images/manual")
    # exclude = ("test", "multisite", "pilot","Kepa")
    # manueal_dcm_sum_df = dcm_dir_summary(manual_lab_project_dir,exclude)
    # # apply set to all the list
    # # manueal_dcm_sum_df = manueal_dcm_sum_df.applymap(lambda x: set(x) if isinstance(x, tuple) else x)
    # # store the dcm_sum_df because it takes so many time...
    # output_dir = "/bcbl/home/public/Gari/VOTCLOC/main_exp/dicom"
    # manueal_dcm_sum_df.to_csv(os.path.join(output_dir,"manual_dicom_check_oct-29.csv"), index=False)
    manueal_dcm_sum_df = pd.read_csv(os.path.join(output_dir, 'manual_dicom_check_Nov-05.csv'))
    # merge the 2 dcm summary
    dcm_merged = pd.concat([dcm_sum_df, manueal_dcm_sum_df], ignore_index=True)
    # summarize the lab note
    lab_note_path = Path('/bcbl/home/public/Gari/VOTCLOC/main_exp/VOTCLOC_subses_list.xlsx')
    lab_note_df = read_lab_note(lab_note_path)

    # merge 2 df to get the corresponding correct sub and session for each dcm dir
    # the info will be stored in a df
    # according to the mapping df, create symlink and echo the session need manual correction
    dcm_merged['_date_list'] = dcm_merged['acq_date'].apply(
        lambda x: x if isinstance(x, set) else {x},
    )
    # this will not work because the reading of csv will not perceve the class set
    dcm_mergedx = dcm_merged.explode('_date_list', ignore_index=True).rename(
        columns={'_date_list': 'date'},
    )
    # apply the dir name parser to sub and ses
    dcm_mergedx['sub_ses'] = dcm_mergedx['dir_name'].apply(process_session)

    # now give label to each of the session
    dcm_mergedx[['dcm_sub', 'dcm_ses']] = dcm_mergedx['sub_ses'].str.extract(
        r'sub-(\d{2})_ses-(\d{2})',
    )
    dcm_mergedx[['ses_suffix', 'ses_label']] = dcm_mergedx['sub_ses'].apply(
        lambda x: pd.Series(get_ses_suffix(x)),
    )

    merged = dcm_mergedx.merge(
        lab_note_df,
        left_on=['dcm_sub', 'date'],
        right_on=['sub', 'date'],
        how='right',
    )

    # start to getting the mapping info
    sub_ses_group = merged.groupby(['sub', 'ses'])
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

                if len(info) > 1:
                    # 1. auto upload and the manual mixed we drop the manual part and check
                    if len(info['lab_project_dir'].unique()) > 1:
                        idx_to_drop = info.index[
                            ~ info['lab_project_dir'].apply(
                                lambda p : str(p).endswith('images'),
                            )
                        ]
                        info = info.drop(idx_to_drop)
                    # we need to compare the info
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
