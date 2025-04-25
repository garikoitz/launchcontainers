"""
MIT License

Copyright (c) 2020-2025 Garikoitz Lerma-Usabiaga
Copyright (c) 2020-2022 Mengxing Liu
Copyright (c) 2022-2023 Leandro Lecca
Copyright (c) 2022-2025 Yongning Lei
Copyright (c) 2023 David Linhardt
Copyright (c) 2023 Iñigo Tellaetxe

Permission is hereby granted, free of charge, to any person obtaining a copy of this software
and associated documentation files (the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.
"""
from __future__ import annotations

import logging
import math
import os
import os.path as op
import subprocess as sp
from subprocess import Popen

import numpy as np

from launchcontainers.prepare_inputs import dask_scheduler_config as dsq
from launchcontainers.prepare_inputs import prepare as prepare
from launchcontainers.prepare_inputs import utils as do
# modules in lc
# for package mode, the import needs to import launchcontainer module

# for testing mode through , we can use relative import
# from prepare_inputs import dask_scheduler_config as dsq
# from prepare_inputs import prepare as prepare
# from prepare_inputs import utils as do


logger = logging.getLogger('Launchcontainers')


# %% launchcontainers
def generate_cmd(
    lc_config, sub, ses, analysis_dir, run_lc,
):
    """Puts together the command to send to the container.

    Args:
        lc_config (str): _description_
        sub (str): _description_
        ses (str): _description_
        analysis_dir (str): _description_
        lst_container_specific_configs (list): _description_
        run_lc (str): _description_

    Raises:
        ValueError: Raised in presence of a faulty config.yaml file, or when the formed command is not recognized.

    Returns:
        _type_: _description_
    """

    # Relevant directories
    # All other relevant directories stem from this one
    basedir = lc_config['general']['basedir']

    homedir = os.path.join(basedir, 'singularity_home')
    container = lc_config['general']['container']
    host = lc_config['general']['host']
    containerdir = lc_config['general']['containerdir']

    # Information relevant to the host and container
    jobqueue_config = lc_config['host_options'][host]
    version = lc_config['container_specific'][container]['version']
    use_module = jobqueue_config['use_module']
    bind_options = jobqueue_config['bind_options']

    # Location of the Singularity Image File (.sif)
    container_name = os.path.join(containerdir, f'{container}_{version}.sif')
    # Define the directory and the file name to output the log of each subject
    container_logdir = os.path.join(analysis_dir, 'sub-' + sub, 'ses-' + ses, 'output', 'log')
    logfilename = f'{container_logdir}/t-{container}-sub-{sub}_ses-{ses}'

    path_to_sub_derivatives = os.path.join(analysis_dir, f'sub-{sub}', f'ses-{ses}')

    bind_cmd = ''
    for bind in bind_options:
        bind_cmd += f'--bind {bind}:{bind} '

    env_cmd = ''
    cmd = (
        f'{env_cmd} '
        f'echo we are testing'
    )
    # If after all configuration, we do not have command, raise an error
    if cmd is None:
        logger.error(
            '\n'
            + 'the DWI PIPELINE command is not assigned, please check your config.yaml[general][host] session\n',
        )
        raise ValueError('cmd is not defined, aborting')

    # GLU: I don't think this is right, run is done below, I will make it work just for local but not in here,
    #      it is good that this function just creates the cmd, I would keep it like that
    if run_lc:
        return (sp.run(cmd, shell=True))
    else:
        return cmd
    #     sp.run(cmd, shell=True)
    # return cmd


# %% the launchcontainer
def launchcontainer(
    analysis_dir,
    lc_config,
    sub_ses_list,
    parser_namespace,
):
    """
    This function launches containers generically in different Docker/Singularity HPCs
    This function is going to assume that all files are where they need to be.

    Args:
        analysis_dir (str): _description_
        lc_config (str): path to launchcontainer config.yaml file
        sub_ses_list (_type_): parsed CSV containing the subject list to be analyzed, and the analysis options
        parser_namespace (argparse.Namespace): command line arguments
    """
    logger.info('\n' + '#####################################################\n')

    # Get the host and jobqueue config info from the config.yaml file
    host = lc_config['general']['host']
    jobqueue_config = lc_config['host_options'][host]
    if host == 'local':
        launch_mode = jobqueue_config['launch_mode']
    logger.debug(f'\n,, this is the job_queue config {jobqueue_config}')

    force = lc_config['general']['force']
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
            'on the prompt before you launch multiple subjects\n'
            '    (3) Once the check is done, launch the jobs by adding --run_lc to the first command you executed.\n',
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
        dwi = row.dwi

        if RUN == 'True':
            # Append config, subject, session, and path info in corresponding lists
            lc_configs.append(lc_config)
            subs.append(sub)
            sess.append(ses)
            dir_analysiss.append(analysis_dir)
            run_lcs.append(run_lc)

            # This cmd is only for print the command
            command = generate_cmd(
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
    paths_to_analysis_config_json,
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
        generate_cmd,
        lc_configs,
        subs,
        sess,
        dir_analysiss,
        paths_to_analysis_config_json,
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


# %% main()
def main():
    parser_namespace, parse_dict = do.get_parser()
    copy_configs = parser_namespace.copy_configs
    # Check if download_configs argument is provided
    if copy_configs:
        # Ensure the directory exists
        if not os.path.exists(copy_configs):
            os.makedirs(copy_configs)
        launchcontainers_version = do.copy_configs(copy_configs)
        # # Use the mocked version function for testing
        # launchcontainers_version = do.get_mocked_launchcontainers_version()

        # if launchcontainers_version is None:
        #     raise ValueError("Unable to determine launchcontainers version.")
        # do.download_configs(launchcontainers_version, download_configs)
    else:
        # Proceed with normal main functionality
        print('Executing main functionality with arguments')
        # Your main function logic here
        # e.g., launch_container(args.other_arg)
    # read ymal and setup the launchcontainer program

        lc_config_path = parser_namespace.lc_config
        lc_config = do.read_yaml(lc_config_path)

        run_lc = parser_namespace.run_lc
        verbose = parser_namespace.verbose
        debug = parser_namespace.debug

        # Get general information from the config.yaml file
        basedir = lc_config['general']['basedir']
        bidsdir_name = lc_config['general']['bidsdir_name']
        containerdir = lc_config['general']['containerdir']
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

        if log_dir == 'analysis_dir':
            log_dir = op.join(
                basedir, bidsdir_name, 'derivatives',
                f'{container}_{version}', f'analysis-{analysis_name}',
            )

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
        logger.critical(
            '\n'
            + '#####################################################\n'
            + f'Successfully read the config file {lc_config_path} \n'
            + f'SubsesList is read, there are {num_of_true_run} jobs needed to be launched'
            + f'Basedir is: {lc_config["general"]["basedir"]} \n'
            + f'Container is: {container}_{lc_config["container_specific"][container]["version"]} \n'
            + f'Host is: {host_str} \n'
            + f'analysis folder is: {lc_config["general"]["analysis_name"]} \n'
            + '##################################################### \n',
        )

        analysis_dir = '/home/tlei/Desktop'

        # Run mode
        launchcontainer(
            analysis_dir,
            lc_config,
            sub_ses_list,
            parser_namespace,
        )


# #%%
if __name__ == '__main__':
    main()
