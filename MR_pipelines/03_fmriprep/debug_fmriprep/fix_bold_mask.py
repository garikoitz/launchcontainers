# """
# MIT License
# Copyright (c) 2024-2025 Yongning Lei
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or substantial
# portions of the Software.
from __future__ import annotations

import os
import subprocess
from multiprocessing import Pool

from bids import BIDSLayout

# Paths
BIDS_DIR = '/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS'
DERIVATIVES_DIR = f'{BIDS_DIR}/derivatives/fmriprep/analysis-forcebbr_minimal_tigeredit'
# BIDS_DIR = "/scratch/tlei/VOTCLOC/BIDS"
# DERIVATIVES_DIR = f"{BIDS_DIR}/derivatives/fmriprep-minimal"
ANTS_CMD = 'antsApplyTransforms'
# Initialize PyBIDS layout
layout = BIDSLayout(DERIVATIVES_DIR, validate=False)


def process_brain_mask(anat_mask, bold_ref, xfm_txt, output_bold_mask):
    """
    Applies ANTs transformation to map a brain mask from T1w space to BOLD space.
    Ensures the correct ANTs module is loaded before execution.
    """
    if os.path.exists(output_bold_mask):
        backup_name = output_bold_mask.replace('.nii.gz', '_backup.nii.gz')
        os.rename(output_bold_mask, backup_name)
        print(f'Renamed existing output: {output_bold_mask} -> {backup_name}')
    # Load the ANTs module before running the command
    module_load_cmd = 'ANTs/2.3.5-foss-2019b-Python-3.7.4'
    # for BCBL it is "module load ants/2.5.1"
    ants_cmd = [
        ANTS_CMD,
        '-d', '3',
        '-i', anat_mask,
        '-r', bold_ref,
        '-t', xfm_txt,
        '-o', output_bold_mask,
        '-n', 'NearestNeighbor',
    ]
    full_cmd = f'{module_load_cmd} && ' + ' '.join(ants_cmd)
    print(f'Processing: {anat_mask} -> {output_bold_mask}')
    print('Running:', full_cmd)
    subprocess.run(full_cmd, shell=True, check=True, executable='/bin/bash')


# Get all subjects and sessions
subjects = layout.get_subjects()
sessions = layout.get_sessions()

jobs = []

for sub in subjects:
    for ses in sessions:
        # Find all anatomical brain masks (space-T1w and desc-brain_mask)
        anat_mask = layout.get(
            subject=sub, session=ses, datatype='anat',
            desc='brain', suffix='mask', extension='nii.gz',
        )[0]
        anat_mask_path = anat_mask.path

        # Find corresponding functional bold reference images
        hmc_boldrefs = layout.get(
            subject=sub, session=ses, datatype='func',
            desc='hmc', suffix='boldref', extension='nii.gz',
        )

        for bold_ref in hmc_boldrefs:
            bold_ref_path = bold_ref.path

            xfm_file = layout.get(
                subject=sub, session=ses, datatype='func',
                task=bold_ref.task, run=bold_ref.run,
                desc='coreg', suffix='xfm', extension='txt',
            )
            xfm_txt_path = xfm_file[0].path
            # Construct the output path
            output_desc_mask = bold_ref_path.replace(
                'desc-hmc_boldref.nii.gz', 'desc-brain_mask.nii.gz',
            )
            if all(map(os.path.exists, [anat_mask_path, bold_ref_path, xfm_txt_path])):
                jobs.append((anat_mask_path, bold_ref_path, xfm_txt_path, output_desc_mask))
            else:
                print(f'Skipping: Missing files for {sub} {ses}')

# Use multiprocessing for faster processing
if __name__ == '__main__':
    with Pool(processes=8) as pool:  # Adjust number of processes
        pool.starmap(process_brain_mask, jobs)

    print('Processing completed.')
