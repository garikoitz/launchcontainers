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
import os.path as op
from os import makedirs
import numpy as np
from scipy import stats
import nibabel as nib
from nilearn.surface import load_surf_data
from nilearn.glm.first_level import (
    make_first_level_design_matrix,
    first_level_from_bids,
)
from nilearn import image
from nilearn.glm.first_level.first_level import run_glm
from nilearn.glm.contrasts import compute_contrast
import nilearn
from bids import BIDSLayout

import argparse
from argparse import RawDescriptionHelpFormatter

import logging
import sys
import concurrent.futures
import csv
import pandas as pd


logger = logging.getLogger("GENERAL")


# %% parser
def get_parser():
    """
    Input:
    Parse command line inputs

    Returns:
    a dict stores information about the cmd input

    """
    parser = argparse.ArgumentParser(
        description="""
        Python script to batch run MINI subjectsurface based fMRI first level analysis        
""",
        formatter_class=RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-base",
        type=str,
        # default="",
        help="basedir e.g. /bcbl/home/public/Gari/VOTCLOC/main_exp",
    )
    parser.add_argument(
        "-i",
        type=str,
        # default="",
        help="input bids dir name, default is BIDS",
    )    

    parser.add_argument(
        "-sub",
        type=str,
        # default="",
        help="subject id, e.g. S005",
    )
    parser.add_argument(
        "-ses",
        type=str,
        # default="",
        help="ses id, e.g. T01",
    )
    parser.add_argument(
        "-fp_ana_name",
        type=str,
        # default="",
        help="analysis name of the fmriprep, the src input to this ",
    )
    parser.add_argument(
        "-output_name",
        type=str,
        # default="",
        help="output folder name ",
    )   
    parser.add_argument(
        "-slice_time_ref",
        type=float,
        default="0.5",
        help="slice timeing, default fmriprep 0.5, we set 0 sometimes ",
        required=False
    )   

    parser.add_argument(
        "-use_smoothed",
        action='store_true',
        help="use_smooth, e.g. True or False",
    )

    parser.add_argument(
        "-sm",
        type=str,
        default="",
        help="freesurfer fwhm smooth factor, 01 02 03 04 05 010",
        required=False
    )




    parse_dict = vars(parser.parse_args())
    parse_namespace = parser.parse_args()

    return parse_dict
### Helper function for saving GIFTI statmaps
def save_statmap_to_gifti(data, outname):
    """Save a statmap to a gifti file.
    data: nilearn contrast model output, e.g., contrast.effect_size()
    outname: output file name
    """
    gii_to_save = nib.gifti.gifti.GiftiImage()
    gii_to_save.add_gifti_data_array(
        nib.gifti.gifti.GiftiDataArray(data=data, datatype="NIFTI_TYPE_FLOAT32")
    )
    nib.save(gii_to_save, outname)


#%%
# Function to replace prefix
def replace_prefix(val):
    if isinstance(val, str) and (val.startswith('EU_') or val.startswith('ES_') or \
        val.startswith('AT_')  or val.startswith('EN_') or val.startswith('FR_') or val.startswith('IT_') or val.startswith('CN_') or val.startswith('ZH_') or val.startswith('JP_')):
        return val[3:]
    else:
        return val



def glm_l1(bids, input_bids, subject, session, fp_ana_name, output_name, slice_time_ref=0.5, use_smoothed=False, sm=None):
    #subject= subject_sessions['BIDS_sub']
    #session= subject_sessions['BIDS_ses']
    # use_smooth: either False 0 or True 01 02 03 04 05 010
    ####
    #####
    #for debug
    # bids='/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS'
    # input_bids= '/bcbl/home/public/Gari/VOTCLOC/main_exp/derivatives/processed_nifti/analysis-newseq'
    # subject='06'
    # session='02'
    # slice_time_ref=0.5
    # fp_ana_name='newseq'
    # output_name='newseq'
    # use_smoothed=False
    ####
    fmriprep_dir = op.join(bids,
        "derivatives", "fmriprep" ,f'analysis-{fp_ana_name}'
    )  # BIDS-relative path to fMRIPrep
    task = "fLoc"  # Task name
    space = "fsnative"  # BOLD projected on subject's freesurfer surface
    hemis = ["L","R"]     #, "R"]  # L for left, R for right
    #use_smoothed = False
    run_nums = ["01","02", "03", "04", "05", "06","07","08","09","10"] # Runs to process
    #slice_time_ref = (
    #    0  #0.5 or 0 From the fMRIPrep command, align slice time correction to start of TR
    #)
    fsdir=f'{fmriprep_dir}/sourcedata/freesurfer'
    
    surf_dir = f"{fsdir}/sub-{subject}/surf"
    label_dir = f"{fsdir}/sub-{subject}/label"

    ### Define output directory
    outdir = op.join(bids, "derivatives", "l1_surface",f"analysis-{output_name}", f'sub-{subject}',f'ses-{session}')

    if not op.exists(outdir):
        makedirs(outdir)



    ### Loop across hemispheres
    for hemi in hemis:
        print("Processing hemi", hemi)
        
        ### Final output dictionary for GLM contrast results (to be combined across runslater)
        contrast_objs = {}
        gii_allrun=[]
        frame_time_allrun=[]
        events_allrun=[]
        confounds_allrun=[]
        store_l1=[]
        ### Loop over runs
        for idx, run_num in enumerate(run_nums):
            print("Processing run", run_num)

            ### Load GIFTI data and z-score it
            run = (
                "run-" + run_num
            )  # Run string in filename (define as empty string "" if no run label)
            func_name = (
                f"sub-{subject}_ses-{session}_task-{task}_{run}_hemi-{hemi}_space-{space}_bold.func.gii"
            )
            # If you smoothed data beforehand, make sure to point this to your smoothed file name!
            print(f"smooth is {use_smoothed}")
            if use_smoothed:
                func_name = func_name.replace("_bold", f"_desc-smoothed{sm}_bold")
            nii_path = op.join(fmriprep_dir, f'sub-{subject}', f"ses-{session}" ,"func", func_name)
            gii_data = load_surf_data(nii_path)
            
            # remove the first 6 volumns of all runs and then concat them
             
            gii_data_float=np.vstack(gii_data[:,:]).astype(float)
            gii_remove_first_6=gii_data_float[:,6::]
            gii_data_std = stats.zscore(gii_remove_first_6, axis=1)
            
            gii_allrun.append(gii_data_std)
        
            # # freesurfer label file
            # label_path=(f'{label_dir}/lh.votcnov1v2.label')
            # mask_votc= load_surf_data(label_path)
            
            
            # ### Get shape of data
            n_vertices = np.shape(gii_data_std)[0]
            n_scans = np.shape(gii_data_std)[1]
            
            # votc_label_dir=f'{label_dir}/lh.votcnov1v2.label'
            # votc_label=load_surf_data(votc_label_dir)
            # mask=np.zeros((n_vertices,1))
            # mask[votc_label]=1
            
            # gii_data_std=gii_data_std*mask
            # gii_data_float=gii_data_float*mask
            #gii_data_std_masked=nilearn.masking.apply_mask(gii_data_std, mask_votc, dtype='f', smoothing_fwhm=None, ensure_finite=True)            
            ### Use the volumetric data just to get the events and confounds file           
            img_filters = [("desc", "preproc")]
            # specify session 
            img_filters.append(("ses", session))
            # If multiple runs are present, then add the run number to filter to specify
            if len(run) > 0:
                img_filters.append(("run", run_num))
            l1 = first_level_from_bids(
                input_bids,
                task,
                space_label="T1w",
                sub_labels=[subject],
                slice_time_ref=slice_time_ref,
                hrf_model="spm",
                drift_model=None,  # Do not high_pass since we use fMRIPrep's cosine regressors
                drift_order=0,  # Do not high_pass since we use fMRIPrep's cosine regressors
                high_pass=None,  # Do not high_pass since we use fMRIPrep's cosine regressors
                img_filters=img_filters,
                derivatives_folder=fmriprep_dir,
            )

            ### Extract information from the prepared model
            t_r = l1[0][0].t_r
            events = l1[2][0][0]  # Dataframe of events information
            confounds = l1[3][0][0]  # Dataframe of confounds
            # get rid of rest so that the setting would be the same as spm
            events_nobaseline=events[events.loc[:,'trial_type']!='baseline']
            events_nobaseline.loc[:,'onset']=events_nobaseline['onset']+idx*(n_scans)*t_r
            
            events_allrun.append(events_nobaseline)
            store_l1.append(l1)
            ### From the confounds file, extract only those of interest
            # Start with the motion and acompcor regressors
            motion_keys = [
                "framewise_displacement",
                "rot_x",
                "rot_y",
                "rot_z",
                "trans_x",
                "trans_y",
                "trans_z",
            ]
            # Get ACompCor components (all to explain 50% variance)
            a_compcor_keys = [key for key in confounds.keys() if "a_comp_cor" in key]

            # Now add non-steady-state volumes
            non_steady_state_keys = [key for key in confounds.keys() if "non_steady" in key]

            # Add cosine regressors which act to high-pass filter data at 1/128 Hz
            cosine_keys = [key for key in confounds.keys() if "cosine" in key]

            # Pull out the confounds we want to keep
            confound_keys_keep = (
                motion_keys + a_compcor_keys + cosine_keys + non_steady_state_keys
            )
            confounds_keep = confounds[confound_keys_keep]

            # Set first value of FD column to the column mean
            confounds_keep["framewise_displacement"][0] = np.nanmean(
                confounds_keep["framewise_displacement"]
            )
            confounds_keep=confounds_keep.iloc[6:]
            confounds_allrun.append(confounds_keep)
            ### Create the design matrix
            # Start by getting times of scans
            frame_times = t_r * ((np.arange(n_scans) + slice_time_ref)+idx*n_scans)
            # Now use Nilearn to create the design matrix from the events files
            frame_time_allrun.append(frame_times)
        
        conc_gii_data_std=np.concatenate(gii_allrun, axis=1)
        concat_frame_times=np.concatenate(frame_time_allrun, axis=0)
        concat_events=pd.concat(events_allrun, axis=0)
	    # Applying the function to the entire DataFrame
        concat_events = concat_events.applymap(replace_prefix)
        concat_confounds=pd.concat(confounds_allrun, axis=0)
        nonan_confounds=concat_confounds.dropna(axis=1, how='any')
        
        design_matrix = make_first_level_design_matrix(
            concat_frame_times,
            events=concat_events,
            hrf_model="spm",  # convolve with SPM's canonical HRF function
            drift_model=None,  # we use fMRIPrep's cosine regressors
            add_regs=nonan_confounds,
        )


        # set the design matrix's NaN value to 0?
        
        # z-score the design matrix to standardize it
        # edited Feb 17 2025, it seems that before the code to form design_matrix std is a to form a array, here I changed it to a dataframe? 
        design_matrix_std = design_matrix.apply(stats.zscore, axis=0)
        # add constant in to standardized design matrix since you cannot z-score a constant
        design_matrix_std["constant"] = np.ones(len(design_matrix_std)).astype(int)
        
        ### Run the GLM
        # Y std or not?
        Y = np.transpose(conc_gii_data_std)
        X = np.asarray(design_matrix_std)
        labels, estimates = run_glm(Y, X, n_jobs=-1)

        ### Define the contrasts
        contrast_matrix = np.eye(design_matrix.shape[1])
        basic_contrasts = dict(
            [
                (column, contrast_matrix[i])
                for i, column in enumerate(design_matrix.columns)
            ]
        )
        contrasts = {
        "AllvsNull": (
            basic_contrasts["face"] 
            + basic_contrasts["bodylimb"] 
            + basic_contrasts["RW"] 
            + basic_contrasts["FF"] 
            + basic_contrasts["CS"] 
            + basic_contrasts["SC"] 
        ),
        "PERvsNull": (
            + basic_contrasts["SC"] 
        ),
        "LEXvsNull": (
            basic_contrasts["CS"] 
            + basic_contrasts["FF"] 
        ),    
        "PERvsLEX": (
            + basic_contrasts["SC"]
            - basic_contrasts["CS"] / 2
            - basic_contrasts["FF"] / 2
        ),          
        "RWvsLEX": (
            basic_contrasts["RW"] 
            - basic_contrasts["CS"] / 2
            - basic_contrasts["FF"] / 2
        ),  
        "RWvsPER": (
            basic_contrasts["RW"] 
            - basic_contrasts["SC"]
        ),  
        "RWvsNull": (
            basic_contrasts["RW"] 
        ), 
        "RWvsLEXPER": (
            basic_contrasts["RW"] 
            - basic_contrasts["CS"] / 3
            - basic_contrasts["FF"] / 3
            - basic_contrasts["SC"] / 3
        ),     
        "RWvsAllnoWordnoLEX": (
            basic_contrasts["RW"] 
            - basic_contrasts["SC"] / 3
            - basic_contrasts["bodylimb"] / 3
            - basic_contrasts["face"] / 3
        ),
        
        "RWvsAllnoWord": (
            basic_contrasts["RW"] 
            - basic_contrasts["CS"] / 5
            - basic_contrasts["FF"] / 5                
            - basic_contrasts["SC"] / 5
            - basic_contrasts["bodylimb"] / 5
            - basic_contrasts["face"] / 5
        ),     
        "LEXvsAllnoLEXnoRW": (
            basic_contrasts["CS"] / 2
            + basic_contrasts["FF"] / 2
            - basic_contrasts["SC"] / 3
            - basic_contrasts["bodylimb"] / 3
            - basic_contrasts["face"] / 3
        ),     
        "PERvsAllnoLEXnoRW": (
            + basic_contrasts["SC"]
            - basic_contrasts["CS"] / 4
            - basic_contrasts["FF"] / 4
            - basic_contrasts["bodylimb"] / 4
            - basic_contrasts["face"] / 4
        ),         
        "CSvsFF": (
            basic_contrasts["CS"] 
            - basic_contrasts["FF"] 
            
        ),     
        "FacesvsNull": (
            basic_contrasts["face"] 
        ),    
        "FacesvsLEX": (
            basic_contrasts["face"]
            - basic_contrasts["CS"] / 2
            - basic_contrasts["FF"]  / 2
        ), 
        "FacesvsPER": (
            basic_contrasts["face"]
            - basic_contrasts["SC"] 
        ),    
        "FacesvsLEXPER": (
            basic_contrasts["face"]
            - basic_contrasts["SC"]  / 3
            - basic_contrasts["CS"] / 3
            - basic_contrasts["FF"]  / 3                
        ),   
        "FacesvsAllnoFace": (
            basic_contrasts["face"]
            - basic_contrasts["SC"]  / 4
            - basic_contrasts["CS"] / 4
            - basic_contrasts["FF"]  / 4  
            - basic_contrasts["bodylimb"] / 4
        ),  
        "LimbsvsNull": (
            basic_contrasts["bodylimb"] 
        ),    
        "LimbsvsLEX": (
            basic_contrasts["bodylimb"] 
            - basic_contrasts["CS"] / 2
            - basic_contrasts["FF"]  / 2
        ), 
        "LimbsvsPER": (
            basic_contrasts["bodylimb"] 
            - basic_contrasts["SC"] 
        ),    
        "LimbsvsLEXPER": (
            basic_contrasts["bodylimb"] 
            - basic_contrasts["SC"]  / 3
            - basic_contrasts["CS"] / 3
            - basic_contrasts["FF"]  / 3                
        ),   
        "LimbsvsAllnoLimbs": (
            basic_contrasts["bodylimb"] 
            - basic_contrasts["SC"]  / 4
            - basic_contrasts["CS"] / 4
            - basic_contrasts["FF"]  / 4 
            - basic_contrasts["face"] / 4           
        ),                                                                                                                                                        
        "RWvsCS": (
             basic_contrasts["RW"] 
            - basic_contrasts["CS"] 
  
        ),        
        "RWvsFF": (
             basic_contrasts["RW"] 
            - basic_contrasts["FF"] 
  
        ),  
        "RWvsFace": (
             basic_contrasts["RW"] 
            - basic_contrasts["face"] 
  
        ),  
        "RWvsLimbs": (
             basic_contrasts["RW"] 
            - basic_contrasts["bodylimb"] 
  
        ),          
        "RWvsSD": (
             basic_contrasts["RW"] 
            - basic_contrasts["SC"] 
  
        )           
    }

            ### Compute the contrasts
        for index, (contrast_id, contrast_val) in enumerate(contrasts.items()):
            # Add a label to the output dictionary if not present
            if contrast_id not in contrast_objs:
                contrast_objs[contrast_id] = []
                
            # Define a name template for output statistical maps (stat-X is replaced later on)
            outname_base_run = f"sub-{subject}_ses-{session}_task-{task}_hemi-{hemi}_space-{space}_contrast-{contrast_id}_stat-X_statmap.func.gii"
            if use_smoothed:
                outname_base_run = outname_base_run.replace(
                    "_statmap", f"_desc-smoothed{sm}_statmap"
                )
            outname_base_run = op.join(outdir, outname_base_run)  # Place in output directory

            # compute contrast-related statistics
            contrast = compute_contrast(
                labels, estimates, contrast_val, contrast_type="t"
            )
            # add contrast to the output dictionary
            contrast_objs[contrast_id].append(contrast)

            # do the run-specific processing
            betas = contrast.effect_size()
            z_score = contrast.z_score()
            t_value = contrast.stat()
            p_value = contrast.p_value()
            variance = contrast.effect_variance()

            # Save the value maps as GIFTIs
            # Effect size
            outname = outname_base_run.replace("stat-X", "stat-effect")
            save_statmap_to_gifti(betas, outname)

            # z-score
            outname = outname_base_run.replace("stat-X", "stat-z")
            save_statmap_to_gifti(z_score, outname)

            # t-value
            outname = outname_base_run.replace("stat-X", "stat-t")
            save_statmap_to_gifti(t_value, outname)

            # p-value
            outname = outname_base_run.replace("stat-X", "stat-p")
            save_statmap_to_gifti(p_value, outname)

            # variance
            outname = outname_base_run.replace("stat-X", "stat-variance")
            save_statmap_to_gifti(variance, outname)
 
    return f"run_glm ingg for {subject} {session}"


def read_tsv(filename):
    with open(filename, newline='') as file:
        reader = csv.DictReader(file, delimiter='\t')
        return list(reader)
'''
import matplotlib.pyplot as plt

from nilearn.plotting import plot_design_matrix

plot_design_matrix(design_matrix)

plt.show()
'''
#%%
def main():
    parser_dict=get_parser()
    basedir=parser_dict['base']
    
    input_dirname=parser_dict['i']
    bids_dir= op.join(basedir,input_dirname)
    input_bids=bids_dir


    subject=parser_dict['sub']
    session=parser_dict['ses']
    use_smoothed=parser_dict['use_smoothed']
    sm=parser_dict['sm']
    
    fp_ana_name=parser_dict['fp_ana_name']
    output_name=parser_dict['output_name']
    slice_time_ref=parser_dict['slice_time_ref']
    #codedir=/bcbl/home/public/Gari/VOTCLOC/VSS/code/05_surface_glm

    # subject="05"
    # session="day1VA" # day1VB day2VA day2VB"
    # fp_ana_name="analysis-okazaki_ST05"
    # output_name="analysis-okazaki_ST05"
    # slice_time_ref=0.5
    
    #subject, session, analysis_name, slice_time_ref=0.5, use_smoothed=False, sm=None
    print(f"----------before going to the glm_l1, smooth is {use_smoothed}")
    #def glm_l1(bids, input_bids, subject, session, fp_ana_name, output_name, slice_time_ref=0.5, use_smoothed=False, sm=None):
    glm_l1(bids_dir, input_bids, subject, session,fp_ana_name, output_name, slice_time_ref, use_smoothed, sm)
    return

if __name__ == "__main__":
    main()
