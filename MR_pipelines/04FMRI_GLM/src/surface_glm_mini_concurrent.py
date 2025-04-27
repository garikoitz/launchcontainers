from __future__ import annotations

import argparse
import logging
import os.path as op
from argparse import RawDescriptionHelpFormatter
from os import makedirs

import nibabel as nib
import numpy as np
from nilearn.glm.contrasts import compute_contrast
from nilearn.glm.first_level import first_level_from_bids
from nilearn.glm.first_level import make_first_level_design_matrix
from nilearn.glm.first_level.first_level import run_glm
from nilearn.surface import load_surf_data
from scipy import stats


logger = logging.getLogger('GENERAL')
import concurrent.futures
import csv


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

    parse_dict = vars(parser.parse_args())
    parse_namespace = parser.parse_args()

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


# %%


def glm_l1(subject_sessions):
    subject = subject_sessions['BIDS_sub']
    session = subject_sessions['BIDS_ses']
    bids = '/bcbl/home/public/Gari/MINI/BIDS_BLQfunc_T1'  # Path to BIDS root
    fmriprep_dir = op.join(
        'derivatives', 'fmriprep' , 'analysis-MINI_DIPC',
    )  # BIDS-relative path to fMRIPrep
    task = 'MINIblock'  # Task name
    space = 'fsnative'  # BOLD projected on subject's freesurfer surface
    hemis = ['L']  # , "R"]  # L for left, R for right
    use_smoothed = False
    run_nums = ['01', '02']  # Runs to process
    slice_time_ref = (
        0.5  # From the fMRIPrep command, align slice time correction to start of TR
    )
    fsdir = '/bcbl/home/public/Gari/MINI/BIDS_BLQfunc_T1/derivatives/fmriprep/analysis-MINI_DIPC/sourcedata/freesurfer'
    surf_dir = f'{fsdir}/sub-{subject}/surf'
    label_dir = f'{fsdir}/sub-{subject}/label'

    # Define output directory
    outdir = op.join(bids, 'derivatives', 'l1_surface', f'{subject}')

    if not op.exists(outdir):
        makedirs(outdir)

    # Loop across hemispheres
    for hemi in hemis:
        print('Processing hemi', hemi)

        # Final output dictionary for GLM contrast results (to be combined across runslater)
        contrast_objs = {}

        # Loop over runs
        for run_num in run_nums:
            print('Processing run', run_num)

            # Load GIFTI data and z-score it
            run = (
                'run-' + run_num
            )  # Run string in filename (define as empty string "" if no run label)
            func_name = (
                f'sub-{subject}_ses-{session}_task-{task}_{run}_hemi-{hemi}_space-{space}_bold.func.gii'
            )
            # If you smoothed data beforehand, make sure to point this to your smoothed file name!
            if use_smoothed:
                func_name = func_name.replace('_bold', '_desc-smoothed_bold')
            nii_path = op.join(
                bids, fmriprep_dir,
                f'sub-{subject}', f'ses-{session}' , 'func', func_name,
            )
            gii_data = load_surf_data(nii_path)
            gii_data_float = np.vstack(gii_data[:, :]).astype(float)

            gii_data_std = stats.zscore(gii_data_float, axis=1)

            # freesurfer label file
            label_path = (f'{label_dir}/lh.votcnov1v2.label')
            mask_votc = load_surf_data(label_path)

            # Get shape of data
            n_vertices = np.shape(gii_data)[0]
            n_scans = np.shape(gii_data)[1]
            votc_label_dir = f'{label_dir}/lh.votcnov1v2.label'
            votc_label = load_surf_data(votc_label_dir)
            mask = np.zeros((n_vertices, 1))
            mask[votc_label] = 1

            gii_data_std = gii_data_std * mask
            gii_data_float = gii_data_float * mask
            # gii_data_std_masked=nilearn.masking.apply_mask(gii_data_std, mask_votc, dtype='f', smoothing_fwhm=None, ensure_finite=True)
            # Use the volumetric data just to get the events and confounds file
            img_filters = [('desc', 'preproc')]
            # If multiple runs are present, then add the run number to filter to specify
            if len(run) > 0:
                img_filters.append(('run', run_num))
            l1 = first_level_from_bids(
                bids,
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
            # get rid of rest so that the setting would be the same as spm
            events_spm = events[events.loc[:, 'trial_type'] != 'rest']
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

            # Create the design matrix
            # Start by getting times of scans
            frame_times = t_r * (np.arange(n_scans) + slice_time_ref)
            # Now use Nilearn to create the design matrix from the events files
            design_matrix = make_first_level_design_matrix(
                frame_times,
                events=events_spm,
                hrf_model='spm',  # convolve with SPM's canonical HRF function
                drift_model=None,  # we use fMRIPrep's cosine regressors
                add_regs=confounds_keep,
            )

            # z-score the design matrix to standardize it
            design_matrix_std = stats.zscore(design_matrix, axis=0)
            # add constant in to standardized design matrix since you cannot z-score a constant
            design_matrix_std['constant'] = np.ones(len(design_matrix_std)).astype(int)

            # Run the GLM
            # Y std or not?
            Y = np.transpose(gii_data_std)
            X = np.asarray(design_matrix)
            labels, estimates = run_glm(Y, X, n_jobs=-1)

            # Define the contrasts
            contrast_matrix = np.eye(design_matrix.shape[1])
            basic_contrasts = {
                column: contrast_matrix[i]
                for i, column in enumerate(design_matrix.columns)
            }
            contrasts = {
                'RWvsConstant': (
                    basic_contrasts['RWH'] / 2
                    + basic_contrasts['RWL'] / 2
                    - basic_contrasts['constant']
                ),
                'FacevsConstant': (
                    basic_contrasts['FC']
                    - basic_contrasts['constant']
                ),
                'RWvsLEX': (
                    basic_contrasts['RWH'] / 2
                    + basic_contrasts['RWL'] / 2
                    - basic_contrasts['FF'] / 3
                    - basic_contrasts['CS'] / 3
                    - basic_contrasts['PW'] / 3
                ),
                'RWvsPER': (
                    basic_contrasts['RWH'] / 2
                    + basic_contrasts['RWL'] / 2
                    - basic_contrasts['CB'] / 3
                    - basic_contrasts['SD'] / 3
                    - basic_contrasts['PS'] / 3
                ),
                'RWvsLEXnoPW': (
                    basic_contrasts['RWH'] / 2
                    + basic_contrasts['RWL'] / 2
                    - basic_contrasts['FF'] / 2
                    - basic_contrasts['CS'] / 2
                ),
                'RWvsAllnoWords': (
                    basic_contrasts['RWH'] / 2
                    + basic_contrasts['RWL'] / 2
                    - basic_contrasts['FF'] / 8
                    - basic_contrasts['CS'] / 8
                    - basic_contrasts['PW'] / 8
                    - basic_contrasts['CB'] / 8
                    - basic_contrasts['SD'] / 8
                    - basic_contrasts['PS'] / 8
                    - basic_contrasts['PF'] / 8
                    - basic_contrasts['FC'] / 8

                ),
                'RWvsAllnoWordsnoFaces': (
                    basic_contrasts['RWH'] / 2
                    + basic_contrasts['RWL'] / 2
                    - basic_contrasts['FF'] / 7
                    - basic_contrasts['CS'] / 7
                    - basic_contrasts['PW'] / 7
                    - basic_contrasts['CB'] / 7
                    - basic_contrasts['SD'] / 7
                    - basic_contrasts['PS'] / 7
                    - basic_contrasts['PF'] / 7

                ),
                'RWvsAllnoWordsnoFacenoPW': (
                    basic_contrasts['RWH'] / 2
                    + basic_contrasts['RWL'] / 2
                    - basic_contrasts['FF'] / 6
                    - basic_contrasts['CS'] / 6
                    - basic_contrasts['CB'] / 6
                    - basic_contrasts['SD'] / 6
                    - basic_contrasts['PS'] / 6
                    - basic_contrasts['PF'] / 6
                ),
                'WordHighvsLEX': (
                    basic_contrasts['RWH']
                    - basic_contrasts['FF'] / 3
                    - basic_contrasts['CS'] / 3
                    - basic_contrasts['PW'] / 3
                ),
                'WordHighvsPER': (
                    basic_contrasts['RWH']
                    - basic_contrasts['CB'] / 3
                    - basic_contrasts['SD'] / 3
                    - basic_contrasts['PS'] / 3
                ),
                'WordHighvsLEXnoPW': (
                    basic_contrasts['RWH']
                    - basic_contrasts['FF'] / 2
                    - basic_contrasts['CS'] / 2
                ),
                'WordHighvsAllnoWords': (
                    basic_contrasts['RWH']
                    - basic_contrasts['FF'] / 8
                    - basic_contrasts['CS'] / 8
                    - basic_contrasts['PW'] / 8
                    - basic_contrasts['CB'] / 8
                    - basic_contrasts['SD'] / 8
                    - basic_contrasts['PS'] / 8
                    - basic_contrasts['PF'] / 8
                    - basic_contrasts['FC'] / 8

                ),
                'WordHighvsAllnoWordsnoFaces': (
                    basic_contrasts['RWH']
                    - basic_contrasts['FF'] / 7
                    - basic_contrasts['CS'] / 7
                    - basic_contrasts['PW'] / 7
                    - basic_contrasts['CB'] / 7
                    - basic_contrasts['SD'] / 7
                    - basic_contrasts['PS'] / 7
                    - basic_contrasts['PF'] / 7

                ),
                'WordHighvsAllnoWordsnoFacenoPW': (
                    basic_contrasts['RWH']
                    - basic_contrasts['FF'] / 6
                    - basic_contrasts['CS'] / 6
                    - basic_contrasts['CB'] / 6
                    - basic_contrasts['SD'] / 6
                    - basic_contrasts['PS'] / 6
                    - basic_contrasts['PF'] / 6
                ),
                'WordLowvsLEX': (
                    basic_contrasts['RWL']
                    - basic_contrasts['FF'] / 3
                    - basic_contrasts['CS'] / 3
                    - basic_contrasts['PW'] / 3
                ),
                'WordLowvsPER': (
                    basic_contrasts['RWL']
                    - basic_contrasts['CB'] / 3
                    - basic_contrasts['SD'] / 3
                    - basic_contrasts['PS'] / 3
                ),
                'WordLowvsLEXnoPW': (
                    basic_contrasts['RWL']
                    - basic_contrasts['FF'] / 2
                    - basic_contrasts['CS'] / 2
                ),
                'WordLowvsAllnoWords': (
                    basic_contrasts['RWL']
                    - basic_contrasts['FF'] / 8
                    - basic_contrasts['CS'] / 8
                    - basic_contrasts['PW'] / 8
                    - basic_contrasts['CB'] / 8
                    - basic_contrasts['SD'] / 8
                    - basic_contrasts['PS'] / 8
                    - basic_contrasts['PF'] / 8
                    - basic_contrasts['FC'] / 8

                ),
                'WordLowvsAllnoWordsnoFaces': (
                    basic_contrasts['RWL']
                    - basic_contrasts['FF'] / 7
                    - basic_contrasts['CS'] / 7
                    - basic_contrasts['PW'] / 7
                    - basic_contrasts['CB'] / 7
                    - basic_contrasts['SD'] / 7
                    - basic_contrasts['PS'] / 7
                    - basic_contrasts['PF'] / 7

                ),
                'WordLowvsAllnoWordsnoFacenoPW': (
                    basic_contrasts['RWL']
                    - basic_contrasts['FF'] / 6
                    - basic_contrasts['CS'] / 6
                    - basic_contrasts['CB'] / 6
                    - basic_contrasts['SD'] / 6
                    - basic_contrasts['PS'] / 6
                    - basic_contrasts['PF'] / 6
                ),
                'FacesvsLEX': (
                    basic_contrasts['FC']
                    - basic_contrasts['FF'] / 3
                    - basic_contrasts['CS'] / 3
                    - basic_contrasts['PW'] / 3
                ),
                'FacesvsPER': (
                    basic_contrasts['FC']
                    - basic_contrasts['CB'] / 3
                    - basic_contrasts['SD'] / 3
                    - basic_contrasts['PS'] / 3
                ),
                'FacesvsLEXnoPW': (
                    basic_contrasts['FC']
                    - basic_contrasts['FF'] / 2
                    - basic_contrasts['CS'] / 2
                ),
                'RWvsPS': (
                    basic_contrasts['RWH'] / 2
                    + basic_contrasts['RWL'] / 2
                    - basic_contrasts['PS']
                ),
            }

            # Compute the contrasts
            for index, (contrast_id, contrast_val) in enumerate(contrasts.items()):
                # Add a label to the output dictionary if not present
                if contrast_id not in contrast_objs:
                    contrast_objs[contrast_id] = []

                # Define a name template for output statistical maps (stat-X is replaced later on)
                outname_base = f'sub-{subject}_ses-{session}_task-{task}_{run}_hemi-{hemi}_space-{space}_contrast-{contrast_id}_stat-X_statmap.func.gii'
                if use_smoothed:
                    outname_base = outname_base.replace(
                        '_statmap', '_desc-smoothed_statmap',
                    )
                outname_base = op.join(outdir, outname_base)  # Place in output directory

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
                outname = outname_base.replace('stat-X', 'stat-effect')
                save_statmap_to_gifti(betas, outname)

                # z-score
                outname = outname_base.replace('stat-X', 'stat-z')
                save_statmap_to_gifti(z_score, outname)

                # t-value
                outname = outname_base.replace('stat-X', 'stat-t')
                save_statmap_to_gifti(t_value, outname)

                # p-value
                outname = outname_base.replace('stat-X', 'stat-p')
                save_statmap_to_gifti(p_value, outname)

                # variance
                outname = outname_base.replace('stat-X', 'stat-variance')
                save_statmap_to_gifti(variance, outname)

        # Now produce the session-wide statistical maps, averaging across all runs
        if len(run_nums) > 1:  # Only do if multiple runs are present
            print('Producing Session-Wide Statistical Maps')
            # Loop across contrast IDs
            for index, (contrast_id, contrast_val) in enumerate(contrasts.items()):
                # Add run-wide contrast objects together
                contrast_concat = contrast_objs[contrast_id][0]
                for i in range(1, len(contrast_objs[contrast_id])):
                    contrast_concat = contrast_concat.__add__(contrast_objs[contrast_id][i])

                # Calculate the statistical maps
                betas = contrast_concat.effect_size()
                z_score = contrast_concat.z_score()
                t_value = contrast_concat.stat()
                p_value = contrast_concat.p_value()
                variance = contrast_concat.effect_variance()

                # Define output name template
                outname_base = f'sub-{subject}_ses-{session}_task-{task}_hemi-{hemi}_space-{space}_contrast-{contrast_id}_stat-X_statmap.func.gii'
                if use_smoothed:
                    outname_base = outname_base.replace('_statmap', '_desc-smoothed_statmap')
                outname_base = op.join(outdir, outname_base)

                # Save the value maps as GIFTIs
                # Effect size
                outname = outname_base.replace('stat-X', 'stat-effect')
                save_statmap_to_gifti(betas, outname)

                # z-score
                outname = outname_base.replace('stat-X', 'stat-z')
                save_statmap_to_gifti(z_score, outname)

                # t-value
                outname = outname_base.replace('stat-X', 'stat-t')
                save_statmap_to_gifti(t_value, outname)

                # p-value
                outname = outname_base.replace('stat-X', 'stat-p')
                save_statmap_to_gifti(p_value, outname)

                # variance
                outname = outname_base.replace('stat-X', 'stat-variance')
                save_statmap_to_gifti(variance, outname)
    return f'run_glm ingg for {subject} {session}'


def read_tsv(filename):
    with open(filename, newline='') as file:
        reader = csv.DictReader(file, delimiter='\t')
        return list(reader)
# %%


def main():
    subseslist = '/bcbl/home/public/Gari/MINI/BIDS_BLQfunc_T1/code/l1_glm/subseslist.tsv'
    subject_sessions = read_tsv(subseslist)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Map the function to the subjects and execute in parallel
        results = list(executor.map(glm_l1, subject_sessions))

        for result in results:
            print(result)


if __name__ == '__main__':
    main()
