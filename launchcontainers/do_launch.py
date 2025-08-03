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
import sys
from datetime import datetime

from launchcontainers import utils as do
from launchcontainers.check import check_dwi_pipelines
from launchcontainers.check import general_checks
from launchcontainers.clusters import dask_scheduler as dask_launch
from launchcontainers.gen_launch_cmd import gen_sub_ses_cmd

logger = logging.getLogger('Launchcontainers')


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
    container = lc_config['general']['container']
    # if use dask, then we will use dask and dask jobqueue
    # if not, we will create tmp files and launch them using sbatch/qsub
    use_dask = lc_config['general']['use_dask']
    if use_dask:
        daskworker_logdir = os.path.join(analysis_dir, 'daskworker_log')

    else:
        # the log file path will go to sp.run returns
        launch_logdir = os.path.join(analysis_dir, f'launch_{container}_log')
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        lc_launch_log = os.path.join(launch_logdir, f'launch_log_{timestamp}.log')
        lc_launch_err = os.path.join(launch_logdir, f'launch_log_{timestamp}.err')

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
        if use_dask:
            dask_launch.print_job_script(host, jobqueue_config , n_jobs, daskworker_logdir)
            logger.critical(f'\n### Example launch command is: {commands[0]}')
        else:
            print(commands)
    # RUN mode
    else:
        if use_dask:
            dask_launch.launch_with_dask(
                jobqueue_config,
                n_jobs,
                daskworker_logdir,
                commands,
            )

        else:
            # TODO:
            # create tmp file useing sge.py, generate the job array from the subseslist
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
    if container in [
        'anatrois',
        'rtppreproc',
        'rtp-pipeline',
        'freesurferator',
        'rtp2-preproc',
        'rtp2-pipeline',
    ]:
        check_dwi_pipelines.check_dwi_analysis_folder(parse_namespace, container)

    # get stuff from subseslist for future jobs scheduling
    sub_ses_list_path = op.join(analysis_dir, 'subseslist.txt')
    df_subses, num_of_true_run = do.read_df(sub_ses_list_path)

    # 3. tree sub-/ses- structure for checking
    # get the first valid sub and ses using tree to show the data structure
    mask = (df_subses['RUN'] == 'True') & (df_subses['dwi'] == 'True')

    # select the first row matching that mask
    first_row = df_subses.loc[mask].iloc[0]

    # extract sub and ses
    sub = first_row['sub']
    ses = first_row['ses']
    logger.critical('\n### output example subject folder structure \n')
    general_checks.cli_show_folder_struc(analysis_dir, sub, ses)

    # 4. ask for user input about folder structure and example command
    general_checks.print_option_for_review(
        num_of_true_run,
        lc_config,
        container,
        bidsdir_name,
    )

    # 5. generate the job script
    # 6. generate command for sample subject
    launch_jobs(
        parse_namespace,
        df_subses,
        num_of_true_run,
        False,
    )
    # === Ask user to confirm before launching anything ===
    ans = input(
        'You are about to launch jobs, please review the'
        "previous session's info. Continue? [y / N]: ",
    )
    if ans.strip().lower() not in ('y', 'yes'):
        logger.info('Aborted by user.')
        sys.exit(0)

    # 7. launch the work
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    logger.critical(f'\n##### The launching time is {timestamp}')
    launch_jobs(
        parse_namespace,
        df_subses,
        num_of_true_run,
        run_lc,
    )
    timestamp_finish = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    logger.critical(f'\n##### The finishing time is {timestamp_finish}')
    # when finished launch QC to read the log and check if everything is there
    return


# # #%%
# if __name__ == '__main__':
#     main()
