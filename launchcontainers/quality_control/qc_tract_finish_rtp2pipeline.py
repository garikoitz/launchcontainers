"""Tract processing and validation functions"""

import pandas as pd
from pathlib import Path
import logging
import os

logger = logging.getLogger(__name__)

def load_tract_params(tractparams_file):
    """
    Load tract parameters from tractparams.tsv
    
    Args:
        tractparams_file (Path): Path to tractparams.tsv file
        
    Returns:
        list: List of tract names
    """
    tractparams_file = Path(tractparams_file)
    
    if not tractparams_file.exists():
        raise FileNotFoundError(f"tractparams.tsv not found: {tractparams_file}")
        
    try:
        df = pd.read_csv(tractparams_file, sep=',')
        exclude = ["Ang", "Sup", "IPS0", "IPS1"]

        df = df[~df["nhlabel"].str.contains("|".join(exclude))]

        tract_names = df["shemi"].astype(str) + "_" + df["nhlabel"].str.replace("-", "_")+'_clean'
        logger.info(f"Loaded {len(tract_names)} tract names")
        return tract_names
        
    except Exception as e:
        logger.error(f"Error reading tractparams.tsv: {e}")
        raise

def get_expected_tract_files():
    """Get expected suffixes for tract files"""
    return [
            '_fa_bin.nii.gz',
            '.tck',
            ]

def check_tract_completeness(subses_outputdir):
    """
    Check if all required tracts are present with expected suffixes
    
    Args:
        subses_outputdir (Path): Path to RTP/mrtrix3 directory
        tract_names (list): List of tract names to check
        
    Returns:
        tuple: (complete_tracts, missing_tracts, tract_status)
    """
    subses_outputdir = Path(subses_outputdir)

    tractparams_file = subses_outputdir / ".." / ".." / ".." / "tractparams_votc-ifg-ipl_280tract_hemi-both.csv"
    tract_names= load_tract_params(tractparams_file)
    
    output_summary_csv = subses_outputdir / "RTP_PIPELINE_ALL_OUTPUT.csv"
    
    if not output_summary_csv.exists():
        return [], tract_names.copy(), {}
    
    output_summary_df= pd.read_csv(output_summary_csv,sep=',')
    if not subses_outputdir.exists():
        return [], tract_names.copy(), {}
        
    expected_suffixes = get_expected_tract_files()
    complete_tracts = []
    missing_tracts = []
    tract_status = {}
    
    for tract_name in tract_names:
        found_suffixes = []
        missing_suffixes = []
        
        #this is for checking in the folder 
        for suffix in expected_suffixes:
            file_path_tracts = subses_outputdir / "tracts" / f"{tract_name}{suffix}"
            file_path_rtp = subses_outputdir / "RTP" / "mrtrix" / f"{tract_name}{suffix}"
            if file_path_tracts.exists() : #or file_path_rtp.exists():
                found_suffixes.append(suffix)
            else:
                missing_suffixes.append(suffix)
        
        # # this is for checking in the csv
        # for suffix in expected_suffixes:
        #     tract_in_csv = f"mrtrix/{tract_name}{suffix}"

        #     if output_summary_df["FileName"].str.contains(tract_in_csv).any():
        #         found_suffixes.append(suffix)
        #     else:
        #         missing_suffixes.append(suffix)


        tract_status[tract_name] = {
            'found': found_suffixes,
            'missing': missing_suffixes,
            'complete': len(found_suffixes) == len(expected_suffixes)
        }
        
        if len(found_suffixes) == len(expected_suffixes):
            complete_tracts.append(tract_name)
        else:
            missing_tracts.append(tract_name)
            
    return complete_tracts, missing_tracts, tract_status


def find_subseslist(analysis_dir):
    for dirpath, dirnames, filenames in os.walk(analysis_dir):
        for fname in filenames:
            if fname.lower() == 'subseslist.txt':
                return os.path.join(dirpath, fname)
    raise FileNotFoundError(f'No subseslist.txt found under {analysis_dir}')

def check_and_tract_dir(subses_outputdir):
    """
    Check for tract directory or zip file and unzip if needed
    
    Args:
        subses_outputdir (Path): Path to the output directory
        
    Returns:
        tuple: (has_tract_dir, has_tract_zip, unzip_success, warning_msg)
    """
    subses_outputdir = Path(subses_outputdir)
    tract_dir = subses_outputdir / "RTP_PIPELINE_ALL_OUTPUT"
    tract_zip = subses_outputdir / "RTP_PIPELINE_ALL_OUTPUT.zip"

    has_tract_dir = tract_dir.exists() and tract_dir.is_dir()
    has_tract_zip = tract_zip.exists() and tract_zip.is_file()
    unzip_success = False
    warning_msg = ""
    
    if has_tract_dir and has_tract_zip:
        logger.info("Both tract/ and tract.zip exist")
        return has_tract_dir, has_tract_zip, True, ""
    elif not has_tract_zip:
        logger.info("output.zip doesn't exist, not finishing")
        
def check_all_sub(analysis_dir):

    path_to_subses = find_subseslist(analysis_dir)
    df_subSes = pd.read_csv(path_to_subses, sep=',', dtype=str)
    for row in df_subSes.itertuples(index=True, name='Pandas'):
        sub = row.sub
        ses = row.ses
        RUN = row.RUN
        dwi = row.dwi
        if RUN == 'True' and dwi == 'True':
            subses_outputdir = os.path.join(
                analysis_dir,
                f'sub-{sub}',
                f'ses-{ses}',
                'output'
            )
            complete_tracts, missing_tracts, tract_status=check_tract_completeness(subses_outputdir)
            if not len(missing_tracts) == 0:
                print(f"sub-{sub}_ses-{ses} have missing tracts {missing_tracts}")