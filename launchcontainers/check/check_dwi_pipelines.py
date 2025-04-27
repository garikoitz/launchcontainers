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

import json
import logging
import os
import os.path as op
import zipfile
logger = logging.getLogger('Launchcontainers')


def check_symlink(path: str) -> None:
    """
    Function to check if a symlink is a link and also if it is being pointed to correct place

    if not point to a real place, the prepare mode will fail

    """
    if op.islink(path):
        if op.exists(path):
            logger.info(
                ' √ Symlink %r is valid and points to %r',
                path, op.realpath(path),
            )
        else:
            target = os.readlink(path)
            logger.error(
                'X Symlink %r is broken (target %r not found)',
                path, target,
            )
            raise FileNotFoundError(f'Broken symlink: {path!r} → {target!r}')

    else:
        logger.info(' %r is not a symlink', path)


def check_tractparam(lc_config, sub, ses, tractparam_df):
    """Checks the correctness of the given parameters.

    Args:
        lc_config (dict): _description_
        sub (str): _description_
        ses (str): _description_
        tractparam_df (pandas.DataFrame): _description_
            inherited parameters: path to the fs.zip file, defined by lc_config, sub, ses.

    Raises:
        FileNotFoundError: _description_

    Returns:
        rois_are_there (bool): Whether the regions of interest (ROIs) are present or not
    """
    # Define the list of required ROIs
    logger.info(
        '\n'
        + '#####################################################\n',
    )
    roi_list = []
    # Iterate over some defined roisand check if they are required or not in the config.yaml
    for col in ['roi1', 'roi2', 'roi3', 'roi4', 'roiexc1', 'roiexc2']:
        for val in tractparam_df[col][~tractparam_df[col].isna()]:
            if '_AND_' in val:
                multi_roi = val.split('_AND_')
                roi_list.extend(multi_roi)
            else:
                if val != 'NO':
                    roi_list.append(val)

    required_rois = set(roi_list)

    # Define the zip file
    basedir = lc_config['general']['basedir']
    container = lc_config['general']['container']
    bidsdir_name = lc_config['general']['bidsdir_name']
    precontainer_anat = lc_config['container_specific'][container]['precontainer_anat']
    anat_analysis_name = lc_config['container_specific'][container]['anat_analysis_name']

    # Define where the fs.zip file is
    fs_zip = op.join(
        basedir,
        bidsdir_name,
        'derivatives',
        f'{precontainer_anat}',
        'analysis-' + anat_analysis_name,
        'sub-' + sub,
        'ses-' + ses,
        'output', 'fs.zip',
    )

    # Extract .gz files from zip file and check if they are all present
    with zipfile.ZipFile(fs_zip, 'r') as zip:
        zip_gz_files = set(zip.namelist())

    # See which ROIs are present in the fs.zip file
    required_gz_files = {f'fs/ROIs/{file}.nii.gz' for file in required_rois}
    logger.info(
        '\n'
        + f'---The following are the ROIs in fs.zip file: \n {zip_gz_files} \n'
        + f'---there are {len(zip_gz_files)} .nii.gz files in fs.zip from anatrois output\n'
        + f'---There are {len(required_gz_files)} ROIs that are required to run RTP-PIPELINE\n',
    )
    if required_gz_files.issubset(zip_gz_files):
        logger.info(
            '\n'
            + '---checked! All required .gz files are present in the fs.zip \n',
        )
    else:
        missing_files = required_gz_files - zip_gz_files
        logger.error(
            '\n'
            + '*****Error: \n'
            + f'there are {len(missing_files)} missed in fs.zip \n'
            + f'The following .gz files are missing in the zip file:\n {missing_files}',
        )
        raise FileNotFoundError('Required .gz file are missing')

    ROIs_are_there = required_gz_files.issubset(zip_gz_files)
    logger.info(
        '\n'
        + '#####################################################\n',
    )
    return ROIs_are_there


def check_dwi_analysis_folder(parse_namespace, container):
    '''
    Check if all the config files are successfully copied

    '''
    # analysis dir path
    analysis_dir = parse_namespace.workdir

    # for the most general 3 things: -lcc -ssl -cc
    ana_dir_lcc = op.join(analysis_dir, 'lc_config.yaml')
    ana_dir_ssl = op.join(analysis_dir, 'subseslist.txt')
    container_configs_fname = f'{container}.json'
    ana_dir_cc = op.join(analysis_dir, container_configs_fname)

    copies = [
        ana_dir_lcc, ana_dir_ssl, ana_dir_cc,
    ]

    general_config_present = all(op.isfile(copy_path) for copy_path in copies)

    if general_config_present:
        logger.critical(
            f'\n### Analysis folder {analysis_dir} is having all the general configs\n'
            + 'Pass to next step',
        )
    else:
        logger.error(
            '\n Did NOT detect back up configs in the analysis folder, \
                Please check then continue the run mode',
        )
        raise FileNotFoundError('Not all the 3 configs is under analysis dir, aborting')

    # 1) Load safely
    with open(ana_dir_cc) as infile:
        config = json.load(infile)

    # 2) check if inputs in the json
    if 'inputs' not in config:
        logger.critical(f"'inputs' field missing; adding to {ana_dir_cc}\n")
        logger.critical('Please check your container version and pay attention to this')

    return general_config_present
