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
from __future__ import annotations

import argparse
import csv
import logging
import os
import os.path as op
import time
from argparse import RawDescriptionHelpFormatter
from os import makedirs

import matplotlib.pyplot as plt
import nibabel as nib
import numpy as np
import pandas as pd
import yaml
from bids import BIDSLayout
from nilearn.glm.contrasts import compute_contrast
from nilearn.glm.first_level import first_level_from_bids
from nilearn.glm.first_level import make_first_level_design_matrix
from nilearn.glm.first_level.first_level import run_glm
from nilearn.plotting import plot_design_matrix
from nilearn.surface import load_surf_data
from scipy import stats

logger = logging.getLogger('GENERAL')


def get_parser():
    """
    Input:
    Parse command line inputs

    Returns:
    a dict stores information about the cmd input

    """
    parser = argparse.ArgumentParser(
        description="""
        Python script to batch run surface based fMRI first-level analysis
""",
        formatter_class=RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '-base',
        type=str,
        # default="",
        help='basedir e.g. /bcbl/home/public/Gari/VOTCLOC/main_exp',
    )
    parser.add_argument(
        '-sub',
        type=str,
        # default="",
        help='subject id, e.g. S005',
    )
    parser.add_argument(
        '-ses',
        type=str,
        # default="",
        help='ses id, e.g. T01',
    )
    parser.add_argument(
        '-fp_ana_name',
        type=str,
        # default="",
        help='analysis name of the fmriprep, the src input to this ',
    )

    parser.add_argument(
        '-task',
        type=str,
        # default="",
        help='task name of the fMRI time series ',
    )
    parser.add_argument(
        '-start_scans',
        type=int,
        # default="",
        help='number of pre_scans in the fMRI time series ',
    )
    parser.add_argument(
        '-space',
        type=str,
        # default="",
        help='Space you want to conduct the experiment: \
        Valid options: T1w func MNI152NLin2009cAsym fsnative fsaverage ',
    )
    parser.add_argument(
        '-contrast',
        type=str,
        # default="",
        help='path to yaml file define contrast ',
    )

    parser.add_argument(
        '-output_name',
        type=str,
        # default="",
        help='output folder name ',
    )
    parser.add_argument(
        '-i',
        type=str,
        default='BIDS',
        help='input bids dir name, default is BIDS',
        required=False,
    )

    parser.add_argument(
        '-slice_time_ref',
        type=float,
        default='0.5',
        help='slice timeing, default fmriprep 0.5, we set 0 sometimes ',
        required=False,
    )

    parser.add_argument(
        '-use_smoothed',
        action='store_true',
        help='use_smooth, e.g. True or False',
    )

    parser.add_argument(
        '-sm',
        type=str,
        default='',
        help='freesurfer fwhm smooth factor, 01 02 03 04 05 010',
        required=False,
    )
    parser.add_argument(
        '-mask',
        type=str,
        default='',
        help='name of label file you want to apply mask \
            it will search at the BIDS/freesurfer',
        required=False,
    )
    parser.add_argument(
        '-selected_runs',
        nargs='+',
        type=int,
        required=False,
        help='List of runs you sepecfied',
    )

    parse_dict = vars(parser.parse_args())
    # parse_namespace = parser.parse_args()

    return parse_dict
# Helper function for saving GIFTI statmaps


def save_statmap_to_gifti(data, outname):
    """Save a statmap to a gifti file.
    data: nilearn contrast model output, e.g., contrast.effect_size()
    outname: output file name
    """
    gii_to_save = nib.gifti.gifti.GiftiImage()
    gii_to_save.add_gifti_data_array(
        nib.gifti.gifti.GiftiDataArray(data=data, datatype='NIFTI_TYPE_FLOAT32'),
    )
    nib.save(gii_to_save, outname)


# Function to replace prefix
def replace_prefix_and_suffix(val):
    if isinstance(val, str) and (val.endswith('1') or val.endswith('2')):
        val=val[:-1]
    if isinstance(val, str) and (
        val.startswith('EU_') or val.startswith('ES_')
        or val.startswith('AT_') or val.startswith('EN_')
        or val.startswith('FR_') or val.startswith('IT_')
        or val.startswith('CN_') or val.startswith('ZH_') or val.startswith('JP_')
    ):
        return val[3:]
    else:
        return val


def glm_l1(
    conc_gii_data_std, design_matrix_std, contrasts,
    bids_dir, task, space, hemi, subject, session,
    output_name, use_smoothed=False, sm=None,
):
    print('------- glm start running')
    # Define output directory
    outdir = op.join(
        bids_dir, 'derivatives', 'l1_surface',
        f'analysis-{output_name}', f'sub-{subject}', f'ses-{session}',
    )

    if not op.exists(outdir):
        makedirs(outdir)

    plot_design_matrix(design_matrix_std)
    plt.savefig(os.path.join(outdir, 'design_matrix.png'))
    # Loop across hemispheres
    Y = np.transpose(conc_gii_data_std)
    X = np.asarray(design_matrix_std)

    labels, estimates = run_glm(Y, X, n_jobs=-1)

    contrast_objs = {}
    # Compute the contrasts
    for index, (contrast_id, contrast_val) in enumerate(contrasts.items()):
        # Add a label to the output dictionary if not present
        if contrast_id not in contrast_objs:
            contrast_objs[contrast_id] = []

        # Define a name template for output statistical maps (stat-X is replaced later on)
        outname_base_run = f'sub-{subject}_ses-{session}_task-{task}_hemi-{hemi}_space-{space}_contrast-{contrast_id}_stat-X_statmap.func.gii'
        if use_smoothed:
            outname_base_run = outname_base_run.replace(
                '_statmap', f'_desc-smoothed{sm}_statmap',
            )
        outname_base_run = op.join(outdir, outname_base_run)  # Place in output directory

        # compute contrast-related statistics
        contrast = compute_contrast(
            labels, estimates, contrast_val, contrast_type='t',
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
        outname = outname_base_run.replace('stat-X', 'stat-effect')
        save_statmap_to_gifti(betas, outname)

        # z-score
        outname = outname_base_run.replace('stat-X', 'stat-z')
        save_statmap_to_gifti(z_score, outname)

        # t-value
        outname = outname_base_run.replace('stat-X', 'stat-t')
        save_statmap_to_gifti(t_value, outname)

        # p-value
        outname = outname_base_run.replace('stat-X', 'stat-p')
        save_statmap_to_gifti(p_value, outname)

        # variance
        outname = outname_base_run.replace('stat-X', 'stat-variance')
        save_statmap_to_gifti(variance, outname)

    finished = 1
    return finished


def prepare_glm_input(
        bids_dir, fmriprep_dir, label_dir, contrast_fpath,
        subject, session, output_name, task, start_scans, hemi, space, slice_time_ref,
        run_list,
        use_smoothed, sm, apply_label_as_mask: None,
):
    '''
    This function is looping for each run of the task to get:
    1. the processed timeseries
    2. events.tsv
    3. the compounds

    2+3 will help tp create design_matrix

    to generate:
    1. the processed timeseries
    2. design_matrix
    3. contrasts

    In the end the calc_glm will need:
    design_matrix
    procesed timeseries
    '''
    # Final output dictionary for GLM contrast results (to be combined across runslater)
    gii_allrun = []
    frame_time_allrun = []
    events_allrun = []
    confounds_allrun = []
    store_l1 = []
    # Loop over runs
    for idx, run_num in enumerate(run_list):
        print('Processing run', run_num)

        # Load GIFTI data and z-score it
        run = (
            'run-' + run_num
        )  # Run string in filename (define as empty string "" if no run label)
        func_name = (
            f'sub-{subject}_ses-{session}_task-{task}_{run}_hemi-{hemi}_space-{space}_bold.func.gii'
        )
        # If you smoothed data beforehand, make sure to point this to your smoothed file name!
        print(f'smooth is {use_smoothed}')
        if use_smoothed:
            func_name = func_name.replace('_bold', f'_desc-smoothed{sm}_bold')
        nii_path = op.join(
            fmriprep_dir, f'sub-{subject}',
            f'ses-{session}' , 'func', func_name,
        )
        gii_data = load_surf_data(nii_path)

        # remove the first 6 volumns of all runs and then concat them
        gii_data_float = np.vstack(gii_data[:, :]).astype(float)
        print(f'length of orig gii is {np.shape(gii_data_float)[1]}')
        # remove prescan
        gii_remove_first_several = gii_data_float[:, start_scans::]
        print(f'length of removed gii is {np.shape(gii_remove_first_several)[1]}')
        gii_data_std = stats.zscore(gii_remove_first_several, axis=1)
        n_vertices = np.shape(gii_data_std)[0]

        if apply_label_as_mask:
            # freesurfer label file
            label_path = (f'{label_dir}/{apply_label_as_mask}')
            surf_mask = load_surf_data(label_path)

            mask = np.zeros((n_vertices, 1))
            mask[surf_mask] = 1

            gii_data_std = gii_data_std * mask
            gii_data_float = gii_data_float * mask
            # gii_data=nilearn.masking.apply_mask(nii_path, surf_mask, dtype='f',
            # smoothing_fwhm=None, ensure_finite=True)

        # Get shape of data
        n_scans = np.shape(gii_data_std)[1]
        gii_allrun.append(gii_data_std)
        # Use the volumetric data just to get the events and confounds file
        img_filters = [('desc', 'preproc')]
        # specify session
        img_filters.append(('ses', session))
        # If multiple runs are present, then add the run number to filter to specify
        if len(run) > 0:
            img_filters.append(('run', run_num))
        l1 = first_level_from_bids(
            bids_dir,
            task,
            space_label='T1w',
            sub_labels=[subject],
            slice_time_ref=slice_time_ref,
            hrf_model='spm',
            drift_model=None,  # Do not high_pass since we use fMRIPrep's cosine regressors
            drift_order=0,  # Do not high_pass since we use fMRIPrep's cosine regressors
            high_pass=None,  # Do not high_pass since we use fMRIPrep's cosine regressors
            img_filters=img_filters,
            derivatives_folder=fmriprep_dir,
        )

        # Extract information from the prepared model
        t_r = l1[0][0].t_r
        events = l1[2][0][0]  # Dataframe of events information
        confounds = l1[3][0][0]  # Dataframe of confounds
        events.loc[:, 'onset'] = events['onset'] + idx * (n_scans) * t_r
        #events_allrun.append(events)

        # get rid of rest so that the setting would be the same as spm
        events_nobaseline = events[events.loc[:, 'trial_type'] != 'baseline']
        events_allrun.append(events_nobaseline)
        store_l1.append(l1)
        # From the confounds file, extract only those of interest
        # Start with the motion and acompcor regressors
        motion_keys = [
            'framewise_displacement',
            'rot_x',
            'rot_y',
            'rot_z',
            'trans_x',
            'trans_y',
            'trans_z',
        ]
        # Get ACompCor components (all to explain 50% variance)
        a_compcor_keys = [key for key in confounds.keys() if 'a_comp_cor' in key]

        # Now add non-steady-state volumes
        non_steady_state_keys = [key for key in confounds.keys() if 'non_steady' in key]

        # Add cosine regressors which act to high-pass filter data at 1/128 Hz
        cosine_keys = [key for key in confounds.keys() if 'cosine' in key]

        # Pull out the confounds we want to keep
        confound_keys_keep = (
            motion_keys + a_compcor_keys + cosine_keys + non_steady_state_keys
        )
        confounds_keep = confounds[confound_keys_keep]

        # Set first value of FD column to the column mean
        confounds_keep['framewise_displacement'][0] = np.nanmean(
            confounds_keep['framewise_displacement'],
        )
        confounds_keep = confounds_keep.iloc[start_scans:]
        confounds_allrun.append(confounds_keep)
        # Create the design matrix
        # Start by getting times of scans
        frame_times = t_r * ((np.arange(n_scans) + slice_time_ref) + idx * n_scans)
        # Now use Nilearn to create the design matrix from the events files
        frame_time_allrun.append(frame_times)

    conc_gii_data_std = np.concatenate(gii_allrun, axis=1)
    concat_frame_times = np.concatenate(frame_time_allrun, axis=0)
    concat_events = pd.concat(events_allrun, axis=0)

    # Applying the function to the entire DataFrame
    concat_events = concat_events.applymap(replace_prefix_and_suffix)
    concat_confounds = pd.concat(confounds_allrun, axis=0)
    nonan_confounds = concat_confounds.dropna(axis=1, how='any')
    # Construct the design matrix
    design_matrix = make_first_level_design_matrix(
        concat_frame_times,
        events=concat_events,
        hrf_model='spm',  # convolve with SPM's canonical HRF function
        drift_model=None,  # we use fMRIPrep's cosine regressors
        add_regs=nonan_confounds,
    )

    # set the design matrix's NaN value to 0?

    # z-score the design matrix to standardize it
    # edited Feb 17 2025, it seems that before the code to form design_matrix std
    # is a to form a array, here I changed it to a dataframe?
    design_matrix_std = design_matrix.apply(stats.zscore, axis=0)
    # add constant in to standardized design matrix since you cannot z-score a constant
    design_matrix_std['constant'] = np.ones(len(design_matrix_std)).astype(int)

    contrasts = load_contrasts(contrast_fpath, design_matrix)
    print(f'\n the basic contrast we have are: {design_matrix.columns}')

    return conc_gii_data_std, design_matrix_std, contrasts


def load_contrasts(yaml_file, design_matrix):
    """
    Loads contrast definitions from a YAML file and converts them into contrast vectors.

    Parameters:
    - yaml_file (str): Path to the YAML file containing contrast definitions.
    - design_matrix (pd.DataFrame): The design matrix with column names.

    Returns:
    - dict: A dictionary where keys are contrast names and values are contrast vectors.
    """
    # Load YAML file
    with open(yaml_file) as f:
        contrast_definitions = yaml.safe_load(f)

    # Create an identity matrix for the design matrix columns
    contrast_matrix = np.eye(design_matrix.shape[1])
    basic_contrasts = {
        column: contrast_matrix[i]
        for i, column in enumerate(design_matrix.columns)
    }

    # Build contrast dictionary
    contrasts = {}
    for contrast_name, conditions in contrast_definitions.items():
        contrast_vector = np.zeros(design_matrix.shape[1])

        # Get counts for normalization
        pos_terms = conditions.get('positive', [])
        neg_terms = conditions.get('negative', [])

        pos_weight = 1 / len(pos_terms) if pos_terms else 0
        neg_weight = -1 / len(neg_terms) if neg_terms else 0  # Negative for subtraction

        # Add positive terms, evenly weighted
        for term in pos_terms:
            if term in basic_contrasts:
                contrast_vector += pos_weight * basic_contrasts[term]

        # Subtract negative terms, evenly weighted
        for term in neg_terms:
            if term in basic_contrasts:
                contrast_vector += neg_weight * basic_contrasts[term]

        # Store in contrasts dictionary
        contrasts[contrast_name] = contrast_vector

    return contrasts


def read_tsv(filename):
    with open(filename, newline='') as file:
        reader = csv.DictReader(file, delimiter='\t')
        return list(reader)


def generate_run_groups(layout, subject, session, task, selected_runs=None):
    """
    Queries BIDS layout for available runs of a task and returns a list of 10 lists,
    each containing `factor` randomly selected runs (with repetition if necessary).

    Parameters:
    - bids_dir (str): Path to the BIDS dataset.
    - task (str): Task name to query runs.

    Returns:
    - List of 10 lists, each containing `factor` runs.
    """

    # Get all unique run numbers for the given task
    if not selected_runs:
        runs = sorted(set(layout.get_runs(subject=subject, session=session, task=task)))
    # runs = [1,2,3,4]
    else:
        runs = selected_runs
    if not runs:
        raise ValueError(f"No runs found for task '{task}' in BIDS dataset.")

    # Convert run numbers to two-digit string format (e.g., "01", "02")
    run_list = [f'{run:02d}' for run in runs]
    print(f'the run list is {run_list}')
    return run_list


'''



'''


def process_run_list(
        bids_dir, fmriprep_dir, label_dir, contrast_fpath,
        subject, session, output_name, task, start_scans, hemi, space, slice_time_ref,
        run_list, use_smoothed, sm, apply_label_as_mask, ):
    print('Processing hemi', hemi)
    print('Processing runs are : ', run_list)
    conc_gii_data_std, design_matrix_std, contrasts = prepare_glm_input(
        bids_dir, fmriprep_dir, label_dir, contrast_fpath,
        subject, session, output_name, task, start_scans, hemi, space, slice_time_ref,
        run_list, use_smoothed, sm, apply_label_as_mask,
    )
    print(f'Contrasts we are using is : {contrasts.keys()}')

    print(f'----------before going to the glm_l1, smooth is {use_smoothed}')
    # if apply_label_as_mask:
    #     run_identifier=f"{run_identifier}_mask-{apply_label_as_mask.spilt('.')[1]}-{apply_label_as_mask.spilt('.')[2]}"
    finished = glm_l1(
        conc_gii_data_std, design_matrix_std, contrasts,
        bids_dir, task, space, hemi, subject, session,
        output_name, use_smoothed, sm,
    )
    return finished


def main():
    '''
    subject='02'
    session='09'
    task='fLoc'
    start_scans=6
    space='fsnative'
    basedir='/bcbl/home/public/Gari/VOTCLOC/main_exp'
    input_dirname='BIDS'
    fp_ana_name='beforeMar05_US'
    output_name='WC-run0506-meeting_test'
    slice_time_ref=0.5
    contrast_fpath='/export/home/tlei/tlei/soft/VOTCLOC/MR_analysis/04_surface_glm/contrast_wordcenter.yaml'
    use_smoothed=False
    sm=None

    apply_label_as_mask=None

    bids_dir = op.join(basedir, input_dirname)
    fsdir=os.path.join(bids_dir, 'derivatives','freesurfer')

    fmriprep_dir = op.join(
        bids_dir,
        'derivatives', f'fmriprep-{fp_ana_name}',
    )

    label_dir = f"{fsdir}/sub-{subject}/label"

    #hemis = ['L']  # , "R"]  # L for left, R for right
    hemi='L'
    selected_runs=[5,6]
    layout = BIDSLayout(bids_dir, validate=False)
    run_list=generate_run_groups(layout,subject, session, task, selected_runs)

    '''
    parser_dict = get_parser()
    basedir = parser_dict['base']

    input_dirname = parser_dict['i']

    subject = parser_dict['sub']
    session = parser_dict['ses']

    # define task and space
    task = parser_dict['task']  # 'fLoc'  # Task name
    start_scans = int(parser_dict['start_scans'])  # number of scans in the begining,
    # fLoc is 6, ret is 5
    space = parser_dict['space']  # 'fsnative'  # BOLD projected on subject's freesurfer surface

    fp_ana_name = parser_dict['fp_ana_name']
    output_name = parser_dict['output_name']
    slice_time_ref = parser_dict['slice_time_ref']
    contrast_fpath = parser_dict['contrast']
    use_smoothed = parser_dict['use_smoothed']
    sm = parser_dict['sm']
    apply_label_as_mask = parser_dict['mask']  # 'lh.mOTS.RWvsPER.label'
    selected_runs = parser_dict['selected_runs']
    print(f'the selected runs are {selected_runs}')
    # define directories
    bids_dir = op.join(basedir, input_dirname)
    fsdir = os.path.join(bids_dir, 'derivatives', 'freesurfer')

    fmriprep_dir = op.join(
        bids_dir,
        'derivatives', f'fmriprep-{fp_ana_name}',
    )

    label_dir = f'{fsdir}/sub-{subject}/label'
    layout = BIDSLayout(bids_dir, validate=False)
    run_list = generate_run_groups(layout, subject, session, task, selected_runs)


    hemis = ["L", "R"]  # L for left, R for right
    for hemi in hemis: #hemi = 'L'

        finished = process_run_list(
            bids_dir, fmriprep_dir, label_dir, contrast_fpath,
            subject, session, output_name, task, start_scans , hemi, space, slice_time_ref,
            run_list, use_smoothed, sm, apply_label_as_mask)

    return


if __name__ == '__main__':
    tic = time.time()
    main()
    toc = time.time()
    print(f'total time of the program is {toc - tic:.4f}')
