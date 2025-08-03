"""
MIT License

Copyright (c) 2020-2023 Garikoitz Lerma-Usabiaga
Copyright (c) 2020-2022 Mengxing Liu
Copyright (c) 2022-2024 Leandro Lecca
Copyright (c) 2022-2023 Yongning Lei
Copyright (c) 2023 David Linhardt
Copyright (c) 2023 Iñigo Tellaetxe

Permission is hereby granted, free of charge,
to any person obtaining a copy of this software and associated documentation files
(the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to permit
persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.
"""
from __future__ import annotations

import logging
import os
import os.path as op
import subprocess as sp
logger = logging.getLogger('Launchcontainers')

def cli_show_folder_struc(analysis_dir, sub, ses):

    # Build the path to that subject/session folder
    path = os.path.join(analysis_dir, f'sub-{sub}', f'ses-{ses}', 'input')

    # Call “tree -C <path>” and let it print directly to your terminal
    sp.run(['tree', '-C', '-L', '3' , path], check=True)


def print_option_for_review(
    num_of_true_run,
    lc_config,
    container,
    bidsdir_name,
):

    basedir = lc_config['general']['basedir']
    host = lc_config['general']['host']
    bids_dname = os.path.join(basedir, bidsdir_name)
    containerdir = lc_config['general']['containerdir']
    version = lc_config['container_specific'][container]['version']
    analysis_name = lc_config['general']['analysis_name']
    deriv_layout = lc_config['general']['deriv_layout'] 
    all_containers = os.listdir(containerdir)
    # add a check to see if container is there
    container_sif_name = f'{container}_{version}.sif'
    container_in_place = container_sif_name in all_containers
    if not container_in_place :
        raise FileNotFoundError(f'No such file : {container_sif_name} \n under {containerdir} ')
    # output the options here for the user to review:
    logger.critical(
        '\n'
        + '#####################################################\n'
        + f'SubsesList is read, there are * {num_of_true_run} * jobs \n '
        + f'Host is {host} \n'
        + f'Basedir is: {basedir} \n'
        + f'Container is:  {container_sif_name}\n'
        + f'singularity image dir is {containerdir} \n'
        + f'analysis name is: {analysis_name} \n'
        + '##################################################### \n',
    )

    if container in ['freesurferator', 'anatrois']:
        src_dir = bids_dname
        logger.critical(f'\n### The source dir is: {src_dir}')

    if container in ['rtppreproc', 'rtp2-preproc']:
        precontainer_anat = lc_config['container_specific'][container]['precontainer_anat']
        anat_analysis_name = lc_config['container_specific'][container]['anat_analysis_name']
        
        if deriv_layout == 'legacy':
            pre_anatrois_dir = op.join(
                basedir,
                bidsdir_name,
                'derivatives',
                f'{precontainer_anat}',
                'analysis-' + anat_analysis_name,
            ) 
        else:
            pre_anatrois_dir = op.join(
                basedir,
                bidsdir_name,
                'derivatives',
                f'{precontainer_anat}_{anat_analysis_name}',
            )

        logger.critical(f'\n ### The source FSMASK and T1w dir: {pre_anatrois_dir}')
    if container in ['rtp-pipeline', 'rtp2-pipeline']:
        # rtppipeline specefic variables
        precontainer_anat = lc_config['container_specific'][container]['precontainer_anat']
        anat_analysis_name = lc_config['container_specific'][container]['anat_analysis_name']
        precontainer_preproc = lc_config['container_specific'][container]['precontainer_preproc']
        preproc_analysis_name = lc_config['container_specific'][container]['preproc_analysis_name']
        # define the pre containers
        if deriv_layout == 'legacy':
            pre_anatrois_dir = op.join(
                basedir,
                bidsdir_name,
                'derivatives',
                f'{precontainer_anat}',
                'analysis-' + anat_analysis_name,
            ) 
        else:
            pre_anatrois_dir = op.join(
                basedir,
                bidsdir_name,
                'derivatives',
                f'{precontainer_anat}_{anat_analysis_name}',
            )

        if deriv_layout == 'legacy':
            pre_preproc_dir = op.join(
                basedir,
                bidsdir_name,
                'derivatives',
                precontainer_preproc,
                'analysis-' + preproc_analysis_name,
            )
        else:
            pre_preproc_dir = op.join(
                basedir,
                bidsdir_name,
                'derivatives',
                f'{precontainer_preproc}_{preproc_analysis_name}',
            )    

        logger.critical(
            f'\n### The source FSMASK and ROI dir is: {pre_anatrois_dir} \n'
            + f'The source DWI preprocessing dir is: {pre_preproc_dir} \n',
        )
    return