# """
# MIT License
# Copyright (c) 2020-2025 Garikoitz Lerma-Usabiaga
# Copyright (c) 2020-2022 Mengxing Liu
# Copyright (c) 2022-2023 Leandro Lecca
# Copyright (c) 2022-2025 Yongning Lei
# Copyright (c) 2023 David Linhardt
# Copyright (c) 2023 Iñigo Tellaetxe
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to permit persons to
# whom the Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.
# """
from __future__ import annotations

import logging
import os
import os.path as op

from bids import BIDSLayout

from launchcontainers import utils as do
from launchcontainers.prepare import prepare_dwi as prep_dwi
# import lc package utilities
logger = logging.getLogger('Launchcontainers')


def setup_analysis_folder(parse_namespace):
    '''
    Description: create analysis folder based on your container and your analysis.

    In the meantime, it will copy your input config files to the analysis folder.

    In the end, it will check if everything are in place and ready for the next level preparation
    which is at the subject and session level

    After this step, the following preparing method will based on the config files
    under the analysis folder instread of your input
    '''
    # read the yaml to get input info
    lc_config_fpath = parse_namespace.lc_config
    lc_config = lc_config = do.read_yaml(lc_config_fpath)
    logger.info('\n setup_analysis_folder reading lc config yaml')
    # read parameters from lc_config
    basedir = lc_config['general']['basedir']
    container = lc_config['general']['container']
    force = lc_config['general']['force']
    analysis_name = lc_config['general']['analysis_name']
    version = lc_config['container_specific'][container]['version']
    bidsdir_name = lc_config['general']['bidsdir_name']
    use_dask = lc_config['general']['use_dask']

    # 1 create container dir and analysis dir
    if container in [
        'anatrois',
        'rtppreproc',
        'rtp-pipeline',
        'freesurferator',
        'rtp2-preproc',
        'rtp2-pipeline',
    ]:

        container_folder = op.join(
            basedir,
            bidsdir_name,
            'derivatives',
            f'{container}_{version}',
        )
        # make dirs
        os.makedirs(container_folder, exist_ok=True)
        # 2 create analysis dir

        analysis_dir = op.join(
            container_folder, f'analysis-{analysis_name}', )
        if not op.isdir(analysis_dir):
            os.makedirs(analysis_dir)

    # 2 create logdir for dask if use dask to launch
    if use_dask:
        host = lc_config['general']['host']
        jobqueue_config = lc_config['host_options'][host]
        daskworer_logdir = os.path.join(analysis_dir, 'daskworker_log')

        if jobqueue_config['manager'] in ['sge', 'slurm'] and not os.path.exists(daskworer_logdir):
            os.makedirs(daskworer_logdir)
        if jobqueue_config['manager'] in ['local']:
            if (jobqueue_config['launch_mode'] == 'dask_worker'):
                os.makedirs(daskworer_logdir)
    else:
        logger.info('Not using dask to lauch task, no dask log dir')

    # 3 Copy the configs
    # define the potential exist config files
    ana_dir_lcc = op.join(analysis_dir, 'lc_config.yaml')
    ana_dir_ssl = op.join(analysis_dir, 'subseslist.txt')
    container_configs_fname = f'{container}.json'
    ana_dir_cc = op.join(analysis_dir, container_configs_fname)

    # copy the config under the analysis folder
    do.copy_file(parse_namespace.lc_config, ana_dir_lcc, force)
    do.copy_file(parse_namespace.sub_ses_list, ana_dir_ssl, force)
    do.copy_file(
        parse_namespace.container_specific_config,
        ana_dir_cc, force,
    )

    logger.debug(
        f'\n The analysis folder: {analysis_dir} successfully created,'
        'all the configs has been copied',
    )

    success = True
    return success


def main(parse_namespace):
    # read the yaml to get input info
    lc_config_fpath = parse_namespace.lc_config
    # read LC config yml
    lc_config = do.read_yaml(lc_config_fpath)
    print('\n cli.main() reading lc config yaml')
    # Get general information from the config.yaml file
    basedir = lc_config['general']['basedir']
    bidsdir_name = lc_config['general']['bidsdir_name']
    container = lc_config['general']['container']
    analysis_name = lc_config['general']['analysis_name']
    version = lc_config['container_specific'][container]['version']
    analysis_dir = op.join(
        basedir, bidsdir_name, 'derivatives',
        f'{container}_{version}', f'analysis-{analysis_name}',
    )

    lc_config_fpath = parse_namespace.lc_config
    # read LC config yml
    lc_config = lc_config = do.read_yaml(lc_config_fpath)
    print('\n do_prepare reading lc config yaml')
    # Get general information from the config.yaml file
    basedir = lc_config['general']['basedir']
    bidsdir_name = lc_config['general']['bidsdir_name']
    # setup the subseslist read it into dataframe
    # get stuff from subseslist for future jobs scheduling
    sub_ses_list_path = parse_namespace.sub_ses_list
    df_subses, num_of_true_run = do.read_df(sub_ses_list_path)

    # the prepare code
    # 1. setup analysis folder
    prepare_step1 = setup_analysis_folder(parse_namespace)

    # 2. do container specific preparation
    #   a. for DWI, prepare the container specific json
    #   b. create symbolic links
    logger.info('Reading the BIDS layout...')
    bids_dname = os.path.join(basedir, bidsdir_name)
    layout = BIDSLayout(bids_dname)
    logger.info('finished reading the BIDS layout.')
    if container in [
        'anatrois',
        'rtppreproc',
        'rtp-pipeline',
        'freesurferator',
        'rtp2-preproc',
        'rtp2-pipeline',
    ]:
        logger.debug(f'{container} is in the list')

        prepare_step2 = prep_dwi.prepare_dwi(parse_namespace, df_subses, layout)
    else:
        logger.error(f'{container} is not in the list')

    logger.critical(
        '\n#####\nAnalysis dir for run mode is \n'
        + f'{analysis_dir}\n',
    )
    return prepare_step1 and prepare_step2
