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
import math
import os
import os.path as op
import re
import subprocess as sp
from datetime import datetime
from subprocess import Popen

import numpy as np
from bids import BIDSLayout

from launchcontainers import config_job_scheduler as dsq
from launchcontainers import parser as lc_parser
from launchcontainers import prepare as prepare
from launchcontainers import utils as do
from launchcontainers.gen_launch_cmd import gen_launch_cmd
# modules in lc
# for package mode, the import needs to import launchcontainer module

# for testing mode using repo; first do a poetry install, then the code will work normally
# from prepare_inputs import dask_scheduler_config as dsq
# from prepare_inputs import prepare as prepare
# from prepare_inputs import utils as do


logger = logging.getLogger('Launchcontainers')


def launchcontainer(
    analysis_dir,
    lc_config,
    sub_ses_list,
    parser_namespace,
    path_to_analysis_container_specific_config,
):
    """
    This function launches containers generically in different Docker/Singularity HPCs
    This function is going to assume that all files are where they need to be.

    Args:
        analysis_dir (str): _description_
        lc_config (str): path to launchcontainer config.yaml file
        sub_ses_list (_type_): parsed CSV containing the subject list to be analyzed,
        and the analysis options
        parser_namespace (argparse.Namespace): command line arguments
    """
    logger.info('\n' + '#####################################################\n')

    # Get the host and jobqueue config info from the config.yaml file
    host = lc_config['general']['host']
    jobqueue_config = lc_config['host_options'][host]
    if host == 'local':
        launch_mode = jobqueue_config['launch_mode']
    logger.debug(f'\n,, this is the job_queue config {jobqueue_config}')

    daskworker_logdir = os.path.join(analysis_dir, 'daskworker_log')

    # Count how many jobs we need to launch from  sub_ses_list
    n_jobs = np.sum(sub_ses_list.RUN == 'True')

    run_lc = parser_namespace.run_lc

    lc_configs = []
    subs = []
    sess = []
    dir_analysiss = []
    paths_to_analysis_config_json = []
    run_lcs = []
    # PREPARATION mode
    if not run_lc:
        logger.critical(
            '\nlaunchcontainers.py was run in PREPARATION mode (without option --run_lc)\n'
            'Please check that: \n'
            '    (1) launchcontainers.py prepared the input data properly\n'
            '    (2) the command created for each subject is properly formed\n'
            '         (you can copy the command for one subject and launch it '
            ' on the prompt before you launch multiple subjects\n'
            '    (3) Once the check is done, launch the jobs by adding '
            '--run_lc to the first command you executed.\n',
        )
        # If the host is not local, print the job script to be launched in the cluster.
        if host != 'local' or (host == 'local' and launch_mode == 'dask_worker'):
            client, cluster = create_cluster_client(jobqueue_config, n_jobs, daskworker_logdir)
            if host != 'local':
                logger.critical(
                    f'The cluster job script for this command is:\n'
                    f'{cluster.job_script()}',
                )
            elif host == 'local' and launch_mode == 'dask_worker':
                logger.critical(
                    f'The cluster job script for this command is:\n'
                    f'{cluster}',
                )
    # Iterate over the provided subject list
    commands = list()
    for row in sub_ses_list.itertuples(index=True, name='Pandas'):
        sub = row.sub
        ses = row.ses
        RUN = row.RUN
        # needs to implement dwi, func etc to control for the other containers

        if RUN == 'True':
            # Append config, subject, session, and path info in corresponding lists
            lc_configs.append(lc_config)
            subs.append(sub)
            sess.append(ses)
            dir_analysiss.append(analysis_dir)
            paths_to_analysis_config_json.append(
                path_to_analysis_container_specific_config[0],
            )
            run_lcs.append(run_lc)

            # This cmd is only for print the command
            command = gen_launch_cmd(
                lc_config,
                sub,
                ses,
                analysis_dir,
                False,  # set to False to print the command
            )
            commands.append(command)
            if not run_lc:
                logger.critical(
                    f'\nCOMMAND for subject-{sub}, and session-{ses}:\n'
                    f'{command}\n\n',
                )

                if not run_lc and lc_config['general']['container'] == 'fmriprep':
                    logger.critical(
                        '\n'
                        'fmriprep now can not deal with session specification, '
                        'so the analysis are running on all sessions of the '
                        'subject you are specifying',
                    )

    # RUN mode
    if run_lc and host != 'local':
        run_dask(
            jobqueue_config,
            n_jobs,
            daskworker_logdir,
            lc_configs,
            subs,
            sess,
            dir_analysiss,
            paths_to_analysis_config_json,
            run_lcs,
        )

    if run_lc and host == 'local':
        if launch_mode == 'parallel':
            k = 0
            njobs = jobqueue_config['njobs']
            if njobs == '' or njobs is None:
                njobs = 2
            steps = math.ceil(len(commands) / njobs)
            logger.critical(
                f'\nLocally launching {len(commands)} jobs in parallel every {njobs} jobs '
                f"in {steps} steps, check your server's memory, some jobs might fail\n",
            )
            for stp in range(steps):
                if stp == range(steps)[-1] and (k + njobs) <= len(commands):
                    selected_commands = commands[k:len(commands)]
                else:
                    selected_commands = commands[k:k + njobs]
                logger.critical(
                    f'JOBS in step {stp+1}:\n{selected_commands}\n',
                )
                procs = [Popen(i, shell=True) for i in selected_commands]
                for p in procs:
                    p.wait()
                k = k + njobs

        elif launch_mode == 'dask_worker':
            logger.critical(
                f'\nLocally launching {len(commands)} jobs with dask-worker, '
                f" keep an eye on your server's memory\n",
            )
            run_dask(
                jobqueue_config,
                n_jobs,
                daskworker_logdir,
                lc_configs,
                subs,
                sess,
                dir_analysiss,
                paths_to_analysis_config_json,
                run_lcs,
            )
        elif launch_mode == 'serial':  # Run this with dask...
            logger.critical(
                f'Locally launching {len(commands)} jobs in series, this might take a lot of time',
            )
            serial_cmd = ''
            for i, cmd in enumerate(commands):
                if i == 0:
                    serial_cmd = cmd
                else:
                    serial_cmd += f' && {cmd}'
            logger.critical(
                f'LAUNCHING SUPER SERIAL {len(commands)} JOBS:\n{serial_cmd}\n',
            )
            sp.run(serial_cmd, shell=True)

    return


def create_cluster_client(jobqueue_config, n_jobs, daskworker_logdir):
    client, cluster = dsq.dask_scheduler(jobqueue_config, n_jobs, daskworker_logdir)
    return client, cluster


def run_dask(
    jobqueue_config,
    n_jobs,
    daskworker_logdir,
    lc_configs,
    subs,
    sess,
    dir_analysiss,
    run_lcs,
):

    client, cluster = create_cluster_client(jobqueue_config, n_jobs, daskworker_logdir)
    logger.info(
        '---this is the cluster and client\n' + f'{client} \n cluster: {cluster} \n',
    )
    print(subs)
    print(sess)
    # Compose the command to run in the cluster
    futures = client.map(
        gen_launch_cmd,
        lc_configs,
        subs,
        sess,
        dir_analysiss,
        run_lcs,
    )
    # Record the progress
    # progress(futures)
    # Get the info and report it in the logger
    results = client.gather(futures)
    logger.info(results)
    logger.info('###########')
    # Close the connection with the client and the cluster, and inform about it
    client.close()
    cluster.close()

    logger.critical('\n' + 'launchcontainer finished, all the jobs are done')
    # return client, cluster


def lc_prepare():
    return


def lc_launch():
    return


def logger_the_option_for_review(
    lc_config_path,
    num_of_true_run,
    lc_config,
    container,
    host_str,
    run_lc,
    force,
    bidsdir_name,
):
    # define some local parameters to be used

    if run_lc:
        mode = 'RUN mode'
    else:
        mode = 'Prepare mode'

    basedir = lc_config['general']['basedir']
    bids_dname = os.path.join(basedir, bidsdir_name)
    version = lc_config['container_specific'][container]['version']
    analysis_name = lc_config['general']['analysis_name']
    # output the options here for the user to review:
    logger.critical(
        '\n'
        + '#####################################################\n'
        + f'Successfully read the config file {lc_config_path} \n'
        + f'SubsesList is read, there are * {num_of_true_run} * jobs needed to be launched'
        + f'Basedir is: {basedir} \n'
        + f'Container is: {container}_{version} \n'
        + f'Host is: {host_str} \n'
        + f'analysis name is: {analysis_name} \n'
        + f'mode: {mode} \n'
        + f'Force is {force}, you choose overwrite: {force} \n'
        + '##################################################### \n',
    )

    if container in ['freesurferator', 'anatrois']:
        src_dir = bids_dname
        logger.critical(f'The source dir is: {src_dir}')
    if container in ['rtppreproc', 'rtp2-preproc']:
        precontainer_anat = lc_config['container_specific'][container]['precontainer_anat']
        anat_analysis_name = lc_config['container_specific'][container]['anat_analysis_name']
        pre_anatrois_dir = op.join(
            basedir,
            bidsdir_name,
            'derivatives',
            f'{precontainer_anat}',
            'analysis-' + anat_analysis_name,
        )

        logger.critical(f'The source FSMASK and T1w dir: {pre_anatrois_dir}')
    if container in ['rtp-pipeline', 'rtp2-pipeline']:
        # rtppipeline specefic variables
        precontainer_anat = lc_config['container_specific'][container]['precontainer_anat']
        anat_analysis_name = lc_config['container_specific'][container]['anat_analysis_name']
        precontainer_preproc = lc_config['container_specific'][container]['precontainer_preproc']
        preproc_analysis_num = lc_config['container_specific'][container]['preproc_analysis_name']
        # define the pre containers
        pre_anatrois_dir = op.join(
            basedir,
            bidsdir_name,
            'derivatives',
            f'{precontainer_anat}',
            'analysis-' + anat_analysis_name,
        )

        pre_preproc_dir = op.join(
            basedir,
            bidsdir_name,
            'derivatives',
            precontainer_preproc,
            'analysis-' + preproc_analysis_num,
        )

        logger.critical(
            f'The source FSMASK and ROI dir is: {pre_anatrois_dir} \n'
            + f'The source DWI preprocessing dir is: {pre_preproc_dir} \n',
        )


def main():
    parser_namespace, parse_dict = lc_parser.get_parser()
    lc_config_path = parser_namespace.lc_config

    print('Executing main function with arguments')

    lc_config = do.read_yaml(lc_config_path)

    run_lc = parser_namespace.run_lc
    verbose = parser_namespace.verbose
    debug = parser_namespace.debug

    # Get general information from the config.yaml file
    basedir = lc_config['general']['basedir']
    bidsdir_name = lc_config['general']['bidsdir_name']
    container = lc_config['general']['container']
    analysis_name = lc_config['general']['analysis_name']
    host = lc_config['general']['host']
    force = lc_config['general']['force']
    print_command_only = lc_config['general']['print_command_only']
    log_dir = lc_config['general']['log_dir']
    log_filename = lc_config['general']['log_filename']

    version = lc_config['container_specific'][container]['version']

    # get stuff from subseslist for future jobs scheduling
    sub_ses_list_path = parser_namespace.sub_ses_list
    sub_ses_list, num_of_true_run = do.read_df(sub_ses_list_path)
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    if log_dir == 'analysis_dir':
        log_dir = op.join(
            basedir, bidsdir_name, 'derivatives',
            f'{container}_{version}', f'analysis-{analysis_name}',
        )

    else:
        log_dir = f'{log_dir}_{container}_{analysis_name}'
    log_filename = f'{log_filename}_{timestamp}'
    do.setup_logger(print_command_only, verbose, debug, log_dir, log_filename)

    # logger the settings
    if host == 'local':
        njobs = lc_config['host_options'][host]['njobs']
        if njobs == '' or njobs is None:
            njobs = 2
        launch_mode = lc_config['host_options']['local']['launch_mode']
        valid_options = ['serial', 'parallel', 'dask_worker']
        if launch_mode in valid_options:
            host_str = (
                f'{host}, \n and commands will be launched in {launch_mode} mode \n'
                f'every {njobs} jobs. '
                f'Serial is safe but it will take longer. '
                f'If you launch in parallel be aware that some of the '
                f'processes might be killed if the limit (usually memory) '
                f'of the machine is reached. '
            )
        else:
            do.die(
                f'local:launch_mode {launch_mode} was passed, valid options are {valid_options}',
            )
    else:
        host_str = f' host is {host}'

    bids_dname = os.path.join(basedir, bidsdir_name)
    logger_the_option_for_review(
        lc_config_path,
        num_of_true_run,
        lc_config,
        container,
        host_str,
        run_lc,
        force,
        bidsdir_name,
    )

    logger.info('Reading the BIDS layout...')
    layout = BIDSLayout(bids_dname)
    logger.info('finished reading the BIDS layout.')

    if re.search(r'raw', bidsdir_name):
        logger.critical('####***Source nifti file are not processed')
    else:
        logger.critical('####***Source nifti file are processed')

    # Prepare file and launch containers
    # First of all prepare the analysis folder: it create you the analysis folder
    # automatically so that you are not messing up with different analysis
    analysis_dir, dict_store_cs_configs = (
        prepare.prepare_analysis_folder(parser_namespace, lc_config)
    )
    path_to_analysis_container_specific_config = dict_store_cs_configs['config_path']

    # Prepare mode
    if container in [
        'anatrois',
        'rtppreproc',
        'rtp-pipeline',
        'freesurferator',
        'rtp2-preproc',
        'rtp2-pipeline',
    ]:  # TODO: define list in another module for reusability accross modules and functions
        logger.debug(f'{container} is in the list')

        prepare.prepare_dwi_input(
            parser_namespace, analysis_dir, lc_config, sub_ses_list, layout, dict_store_cs_configs,
        )
    else:
        logger.error(f'{container} is not in the list')

    # Run mode
    launchcontainer(
        analysis_dir,
        lc_config,
        sub_ses_list,
        parser_namespace,
        path_to_analysis_container_specific_config,
    )
    return


# #%%
if __name__ == '__main__':
    main()
