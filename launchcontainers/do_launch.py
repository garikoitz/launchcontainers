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
import subprocess as sp
import sys

from launchcontainers import utils as do
from launchcontainers.check import check_dwi_pipelines as check
from launchcontainers.clusters import dask_scheduler as daskq
from launchcontainers.gen_launch_cmd import gen_sub_ses_cmd

logger = logging.getLogger('Launchcontainers')


def show_first_tree(analysis_dir, sub, ses):

    # Build the path to that subject/session folder
    path = os.path.join(analysis_dir, f'sub-{sub}', f'ses-{ses}')

    # Call “tree -C <path>” and let it print directly to your terminal
    sp.run(['tree', '-C', path], check=True)


def print_option_for_review(
    num_of_true_run,
    lc_config,
    container,
    bidsdir_name,
):

    basedir = lc_config['general']['basedir']
    host = lc_config['general']['host']
    bids_dname = os.path.join(basedir, bidsdir_name)
    version = lc_config['container_specific'][container]['version']
    analysis_name = lc_config['general']['analysis_name']
    # output the options here for the user to review:
    logger.critical(
        '\n'
        + '#####################################################\n'
        + f'SubsesList is read, there are * {num_of_true_run} * jobs '
        + f'Host is {host}'
        + f'Basedir is: {basedir} \n'
        + f'Container is: {container}_{version} \n'
        + f'analysis name is: {analysis_name} \n'
        + '##################################################### \n',
    )

    if container in ['freesurferator', 'anatrois']:
        src_dir = bids_dname
        logger.critical(f'\n### The source dir is: {src_dir}')
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
    return


def print_job_script(host, jobqueue_config , n_jobs, daskworker_logdir):
    if host == 'local':
        launch_mode = jobqueue_config['launch_mode']
    # If the host is not local, print the job script to be launched in the cluster.
    if host != 'local' or (host == 'local' and launch_mode == 'dask_worker'):
        _, cluster = daskq.dask_scheduler(jobqueue_config, n_jobs, daskworker_logdir)
        if host != 'local':
            logger.critical(
                f'Cluster job script for this command is:\n'
                f'{cluster.job_script()}',  # type: ignore
            )
        elif host == 'local' and launch_mode == 'dask_worker':
            logger.critical(
                f'Local job script by dask is:\n'
                f'{cluster}',
            )
        else:
            logger.critical(
                'Job launched on local, no job script',
            )
    return


def run_cmd(cmd: str):
    return sp.run(cmd, shell=True).returncode


def launch_with_dask(jobqueue_config, n_jobs, daskworker_logdir, cmds):

    client, cluster = daskq.dask_scheduler(jobqueue_config, n_jobs, daskworker_logdir)
    logger.info(
        '---this is the cluster and client\n' + f'{client} \n cluster: {cluster} \n',
    )

    # Compose the command to run in the cluster
    futures = client.map(  # type: ignore
        run_cmd,
        cmds,
    )
    results = client.gather(futures)  # type: ignore
    logger.info(results)
    logger.info('###########')
    # Close the connection with the client and the cluster, and inform about it
    client.close()  # type: ignore
    cluster.close()  # type: ignore

    logger.critical('\n' + 'launchcontainer finished, all the jobs are done')
    return


def launch_jobs(
    parse_namespace,
    df_subses,
    num_of_true_run,
    run_lc,
):
    """
    """
    # read LC config yml from analysis dir
    analysis_dir = parse_namespace.workdir

    lc_config_fpath = op.join(analysis_dir, 'lc_config.yaml')
    lc_config = do.read_yaml(lc_config_fpath)
    host = lc_config['general']['host']
    jobqueue_config = lc_config['host_options'][host]

    use_dask = lc_config['general']['use_dask']
    if use_dask:
        daskworker_logdir = os.path.join(analysis_dir, 'launch_log')

    else:
        print('Not implement yet')
        # launch_logdir = os.path.join(analysis_dir, 'launch_log')
        # timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        # dot_o = os.path.join(launch_logdir, f'launch_log_{timestamp}.log')
        # dot_e = os.path.join(launch_logdir, f'launch_log_{timestamp}.err')

    n_jobs = num_of_true_run
    # Iterate over the provided subject list
    commands = []
    lc_configs = []
    subs = []
    sess = []
    dir_analysiss = []

    for row in df_subses.itertuples(index=True, name='Pandas'):
        sub = row.sub
        ses = row.ses
        RUN = row.RUN
        dwi = row.dwi
        # needs to implement dwi, func etc to control for the other containers

        if RUN == 'True' and dwi == 'True':
            # This cmd is only for print the command
            command = gen_sub_ses_cmd(
                lc_config, sub, ses, analysis_dir,
            )
            commands.append(command)
            lc_configs.append(lc_config)
            subs.append(sub)
            sess.append(ses)
            dir_analysiss.append(analysis_dir)

    if not run_lc:
        logger.critical('\n### No launching, here is the command line command')
        print_job_script(host, jobqueue_config , n_jobs, daskworker_logdir)
        logger.critical(f'\n### Example launch command is: {commands[0]}')

    # RUN mode
    else:
        if use_dask:
            launch_with_dask(
                jobqueue_config,
                n_jobs,
                daskworker_logdir,
                commands,
            )

        else:
            pass

    return


def main(parse_namespace):
    # 1. setup run mode logger
    # read the yaml to get input info
    analysis_dir = parse_namespace.workdir
    run_lc = parse_namespace.run_lc

    # read LC config yml from analysis dir
    lc_config_fpath = op.join(analysis_dir, 'lc_config.yaml')
    lc_config = do.read_yaml(lc_config_fpath)
    print('\n cli.main() reading lc config yaml')
    # Get general information from the config.yaml file
    bidsdir_name = lc_config['general']['bidsdir_name']
    container = lc_config['general']['container']

    # 2. do a independent check to see if everything is in place
    check.check_dwi_analysis_folder(parse_namespace, container)

    # 3. ask for user input about folder structure and example command
    # get stuff from subseslist for future jobs scheduling
    sub_ses_list_path = op.join(analysis_dir, 'subseslist.txt')
    df_subses, num_of_true_run = do.read_df(sub_ses_list_path)
    print_option_for_review(
        num_of_true_run,
        lc_config,
        container,
        bidsdir_name,
    )
    # 4. tree sub-/ses- structure for checking
    # get the first valid sub and ses using tree to show the data structure
    mask = (df_subses['RUN'] == 'True') & (df_subses['dwi'] == 'True')

    # select the first row matching that mask
    first_row = df_subses.loc[mask].iloc[0]

    # extract sub and ses
    sub = first_row['sub']
    ses = first_row['ses']
    logger.critical('\n### output example subject folder structure \n')
    show_first_tree(analysis_dir, sub, ses)

    # 5. generate the job script
    # 6. generate command for sample subject

    launch_jobs(
        parse_namespace,
        df_subses,
        num_of_true_run,
        False,
    )

    # host = lc_config['general']['host']
    # # logger the settings
    # if host == 'local':
    #     njobs = lc_config['host_options'][host]['njobs']
    #     if njobs == '' or njobs is None:
    #         njobs = 2
    #     launch_mode = lc_config['host_options']['local']['launch_mode']
    #     valid_options = ['serial', 'parallel', 'dask_worker']
    #     if launch_mode in valid_options:
    #         host_str = (
    #             f'{host}, \n and commands will be launched in {launch_mode} mode \n'
    #             f'every {njobs} jobs. '
    #             f'Serial is safe but it will take longer. '
    #             f'If you launch in parallel be aware that some of the '
    #             f'processes might be killed if the limit (usually memory) '
    #             f'of the machine is reached. '
    #         )
    #     else:
    #         do.die(
    #             f'local:launch_mode {launch_mode} was passed, valid options are {valid_options}',
    #         )
    # else:
    #     host_str = f' host is {host}'

    # === Ask user to confirm before launching anything ===

    ans = input(
        'You are about to launch jobs, please review the'
        "previous session's info. Continue? [y / N]: ",
    )
    if ans.strip().lower() not in ('y', 'yes'):
        logger.info('Aborted by user.')
        sys.exit(0)

    # 7. launch the work
    launch_jobs(
        parse_namespace,
        df_subses,
        num_of_true_run,
        run_lc,
    )
    return


# # #%%
# if __name__ == '__main__':
#     main()
