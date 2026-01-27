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
from typing import Tuple, List

import pandas as pd
import typer

app = typer.Typer()

@app.command()
def read_lab_note(lab_note_path: Path):
    '''
    code for read_lab_xlsx
    
    :param lab_note_path: Description
    :type lab_note_path: Path
    '''
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

def parse_dcm_ses_name(name: str):
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

def extract_sesid_suffix(name: str):
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
    elif suffix in ['failed','-02','lost','bad'] or 'wrong' in suffix :
        label = 'wrong'
    else:
        label = 'seperate'
    return ses, suffix , label


def parse_dicom_metadata(
    summary_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Parse and clean DICOM directory metadata from CSV summaries.
    
    This function loads DICOM summary dataframe and extracts
    structured metadata including subject/session identifiers, acquisition dates,
    and quality flags for BIDS conversion workflows.
    
    Parameters
    ----------
    summary_df :Summary df
    
    Returns
    -------
    pd.DataFrame
        Cleaned and expanded DICOM metadata with the following columns:
        
        Original columns from input CSVs:
        - lab_project_dir: Path to session folder containing all DICOM files
        - dir_name: Name of the lab_project_dir
        - levels_from_top: Number of directory levels for heudiconv wildcards
        - number_of_protocol: Number of runs in the session
        - example_file_name: Example DICOM filename (e.g., '127.dcm')
        - session_correct: Flag (1/0) indicating if session has mixed DICOM files
        - acq_date: Acquisition date(s) in format YYYY-MM-DD
        - acq_time: Acquisition time in format HH:MM:SS
        
        Derived columns:
        - origin: Source of DICOM data ('base' or 'manual')
        - sub: Subject ID extracted from dir_name
        - ses_id_from_dcm: Initial session guess from dir_name parsing
        - ses: Cleaned session identifier
        - suffix: Additional session suffix (if any)
        - note: Quality flag ('wrong' if multiple dates or session_correct==0)
        - date: Expanded acquisition date (one row per date if multiple)
    
    Notes
    -----
    - Sessions with multiple acquisition dates are flagged as 'wrong' in the note column
    - Sessions with session_correct==0 are also flagged as 'wrong'
    - The returned DataFrame is expanded so that sessions with multiple acquisition
      dates appear as separate rows (one per date)
    - Requires helper functions: guess_subsesid_from_dcm() and extract_sesid_suffix()
    """
    # Extract origin (base vs manual)
    summary_df['origin'] = summary_df['lab_project_dir'].apply(
        lambda x: 'manual' if 'manual' in str(x).lower() else 'base',
    )
    
    # Parse subject and session IDs from directory name
    summary_df[['sub', 'ses_id_from_dcm']] = (
        summary_df['dir_name']
        .apply(parse_dcm_ses_name)
        .apply(pd.Series)
    )
    
    # Separate session guess into components
    summary_df[['ses', 'suffix', 'note']] = (
        summary_df['ses_id_from_dcm']
        .apply(extract_sesid_suffix)
        .apply(pd.Series)
    )
    
    # Parse acquisition dates (convert from string representation of list)
    summary_df['acq_date'] = (
        summary_df['acq_date'].astype(str).str.strip("{}").str.replace("'", '').str.split(',')
    )

    
    # Flag sessions with multiple acquisition dates as problematic
    summary_df.loc[summary_df['acq_date'].apply(len) > 1, 'note'] = 'wrong'
    
    # Flag sessions with incorrect DICOM organization
    summary_df.loc[summary_df['session_correct'] == 0, 'note'] = 'wrong'
    
    # Expand rows: one row per acquisition date
    dcm_expand = summary_df.explode('acq_date').reset_index(drop=True)
    
    # Create clean date column
    dcm_expand['date'] = dcm_expand['acq_date']
    
    return dcm_expand


def validate_dicom_metadata(dcm_df: pd.DataFrame) -> Tuple[int, int, List[str]]:
    """
    Validate parsed DICOM metadata and return summary statistics.
    
    Parameters
    ----------
    dcm_df : pd.DataFrame
        DataFrame returned from parse_dicom_metadata()
    
    Returns
    -------
    tuple
        - n_total: Total number of sessions (after date expansion)
        - n_problematic: Number of sessions flagged as 'wrong'
        - problematic_subjects: List of unique subjects with problematic sessions
    
    Examples
    --------
    >>> dcm_df = parse_dicom_metadata('base.csv', 'manual.csv')
    >>> n_total, n_bad, bad_subs = validate_dicom_metadata(dcm_df)
    >>> print(f"Found {n_bad}/{n_total} problematic sessions")
    >>> print(f"Affected subjects: {bad_subs}")
    """
    n_total = len(dcm_df)
    problematic = dcm_df[dcm_df['note'] == 'wrong']
    n_problematic = len(problematic)
    problematic_subjects = problematic['sub'].unique().tolist()
    
    return n_total, n_problematic, problematic_subjects



@app.command()
def main():
    # get the lab note
    lab_note_path = Path('/bcbl/home/public/Gari/VOTCLOC/main_exp/VOTCLOC_subses_list.xlsx')
    lab_note = read_lab_note(lab_note_path)
    # rename the ses col
    lab_note.rename(columns={'ses': 'ses_id_from_xlsx'}, inplace=True)
    lab_note[['ses', 'suffix', 'note']] = lab_note['ses_id_from_xlsx'].apply(extract_sesid_suffix).apply(pd.Series)
    # clean the ses we are not using
    # keywords = ['t', 'lost', 'wrong', 'ME', 'failed', 'No', 'bad','-']
    # pattern = '|'.join(keywords)
    # lab_note = lab_note[~lab_note['ses'].str.contains(pattern, case=False, na=False)]
    

    # read and merge the 2 dcm summary
    dcm_dir = Path('/bcbl/home/public/Gari/VOTCLOC/main_exp/dicom')
    output_dir = Path('/bcbl/home/public/Gari/VOTCLOC/main_exp/dicom')
    dcm_sum_df = pd.read_csv(os.path.join(dcm_dir, 'base_dicom_check_Nov-05.csv'))
    manual_dcm_sum_df = pd.read_csv(os.path.join(dcm_dir, 'manual_dicom_check_Nov-05.csv'))
    dcm_merged = pd.concat([dcm_sum_df, manual_dcm_sum_df], ignore_index=True)

    # those 2 df storing the information from the dcm_dir_summary, it have columns:
    # lab_project_dir: the directory to the session folder stores all dicom
    # dir_name: the name of the lab_project_dir    
    # levels_from_top: will be used in heudiconv to decide how many * will be used
    # number_of_protocal: number of runs in the session
    # example_file_name: 127.dcm
    # session_correct : 1 is this session have mixed dcm files under the same protocal folder?
    # acq_date: 2025-02-11 
    # acq_time: 16:30:05 

    # below we need to clean the dcm info df to extract the information for checking
    # apply the dir name parser to sub and ses
    dcm_expand=parse_dicom_metadata(dcm_merged)

    # dcm_expand.loc[(dcm_expand['sub']=='05') & (dcm_expand['ses']=='08'),:]
    # check the ones with suffix
    # dcm_merged[dcm_merged['ses_id_from_dcm'].apply(lambda x: len(x)>2)]

    # make sure the sub ses and date are all str
    lab_note['sub'] = lab_note['sub'].astype(str).str.zfill(2)
    lab_note['ses'] = lab_note['ses'].astype(str)
    lab_note['date'] = lab_note['date'].astype(str).str.strip()
    
    dcm_expand['sub'] = dcm_expand['sub'].astype(str).str.zfill(2)
    #dcm_expand['ses'] = dcm_expand['ses_dcm'].astype(str)
    dcm_expand['date'] = dcm_expand['date'].astype(str).str.strip()

    # merge 2 df to get the corresponding correct sub and session for each dcm dir
    merged = pd.merge(
        lab_note,
        dcm_expand, 
        on=['sub','date'], 
        how = 'outer',
        suffixes=('_from_note', '_from_dcm'),
        indicator=True
    )

    # drop the rows that have problems identified in the lab note

    merged = merged[~(merged['note_from_note'].isin(['wrong']) | merged['note_from_dcm'].isin(['wrong']))]
    merged = merged.reset_index(drop=True)
    merged = merged.

    # manual edit:
    # merged.loc[merged['_merge']=='right_only',['sub', 'ses_from_dcm','ses_id_from_dcm','date']]
    # checking right_only sub and ses with the lab_note manualy
    # sub-03_ses-04 Jan-29 DWI session --> add both to the _merge column

    # base, VOTCLOC_S6T02 2025-01-16 is very wrong, 158 protocal

    # base, VOTCLOC_S05T02 2024-12-05 is wrong, it should be point to sub-08 ses--01

    # VOTCLOC_S8T03/base VOTCLOC_S8T01/manual VOTCLOC_S08T03/base are all. sub-08_ses-01
    # they are all from 2024-12-20 we cann drop the 2 S8T03 and the S08T03

    merged.sort_values(['sub','ses_from_note']).to_csv('/bcbl/home/public/Gari/VOTCLOC/main_exp/dcm_bids_mapping_summary.csv', index=False)

'''
Note: sub-01_ses-01 under image folder have missing files
sub-05_ses-04 have a 0212 session that have all the structural data
sub-03_ses--02 need to delete it
sub-03_ses-032 is from manual, need to delete it from the csv
'''
