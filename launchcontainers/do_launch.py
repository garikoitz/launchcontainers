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
from os import makedirs
import os
import os.path as op
import subprocess as sp
import sys
from datetime import datetime

from launchcontainers import utils as do
from launchcontainers.check import check_dwi_pipelines
from launchcontainers.check import general_checks
from launchcontainers.clusters import dask_scheduler as dask_launch
from launchcontainers.clusters import sge
from launchcontainers.clusters import slurm
from launchcontainers.gen_launch_cmd import gen_launch_cmd

logger = logging.getLogger('Launchcontainers')

def write_job_script(job_script, script_dir, job_script_fname):
    """Submit SLURM job by writing script to file first"""
    
    # Create script directory if specified
    if script_dir:
        makedirs(script_dir, exist_ok=True)
        job_script_fpath = op.join(script_dir, f"{job_script_fname}.sh")

    # Write script to file
    with open(job_script_fpath, 'w') as f:
        f.write(job_script)
    
    # Make executable
    os.chmod(job_script_fpath, 0o755)

    return job_script_fpath
def launch_jobs(
    parse_namespace,
    df_subses,
    container_log_dir,
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
    # if use dask, then we will use dask and dask jobqueue
    # if not, we will create tmp files and launch them using sbatch/qsub
    use_dask = lc_config['general']['use_dask']
    if use_dask:
        daskworker_logdir = os.path.join(analysis_dir, 'dask_log')
        os.makedirs(daskworker_logdir, exist_ok=True)
    # get number of jobs from subseslist
    n_jobs = len(df_subses)
    # write commands in to a single file to form batch array
    batch_command_fpath = op.join(container_log_dir, 'batch_commands.txt')
    # create job_script_fname to get the batch job script
    job_script_fname =  'src_launch_script.txt'
    commands = gen_launch_cmd(parse_namespace, df_subses, batch_command_fpath)
    # read the commands from the command array using python 
    array_id = 1
    with open(batch_command_fpath) as f:
        lines = f.readlines()
    command = lines[array_id - 1].strip()
    # if it is dry run mode
    if not run_lc:
        logger.critical('\n### No launching, here is the launching command')
        if use_dask:
            # print the job_script generate by dask
            dask_launch.print_job_script(host, jobqueue_config , n_jobs, daskworker_logdir)
        else:
            if host == 'DIPC':
                job_script = slurm.gen_slurm_array_job_script(
                    parse_namespace,
                    container_log_dir,
                    n_jobs,
                )
                logger.critical(f'\n### SLURM job script is {job_script}')
            elif host == 'BCBL':
                job_script = sge.gen_sge_array_job_script(
                    parse_namespace,
                    container_log_dir,
                    n_jobs,
                )
                logger.critical(f'\n### SGE job script is {job_script}')
        # finally cat the from the command
        logger.critical(f'\n### Example launch command is: {command}')

    # RUN mode
    else:
        logger.critical('\n### Real running , here is the launching command')

        if use_dask:
            dask_launch.launch_with_dask(
                jobqueue_config,
                n_jobs,
                daskworker_logdir,
                commands,
            )

        else:
            def launch_cmd(cmd):
                result = sp.run(cmd, shell=True, capture_output=True, text=True, timeout=60)
                return result.returncode

            if host == 'DIPC':
                batch_command = f"""$(sed -n "${{SLURM_ARRAY_TASK_ID}}p" {batch_command_fpath})"""
                job_script = slurm.gen_slurm_array_job_script(
                    parse_namespace,
                    container_log_dir,
                    n_jobs,
                )
                final_script = job_script.replace('your_command_here', batch_command)
                # Submit job
                job_script_fpath = write_job_script(final_script,container_log_dir,job_script_fname)
                logger.critical(
                    f'This is the final job script that is being lauched: \n {final_script}',
                )
                cmd = f"sbatch {job_script_fpath}"
                try:
                    return_code = launch_cmd(cmd)
                    logger.critical(f'\n return code of launch is {return_code} \n')
                except sp.TimeoutExpired:
                    logger.critical("❌ Sbatch submission timed out!")
                    return 1, "", "Submission timeout"
                except Exception as e:
                    logger.critical(f"❌ Error during submission: {e}")
                    return 1, "", str(e)                    
            elif host == 'BCBL':
                batch_command = f"""$(sed -n "${{SGE_TASK_ID}}p" {batch_command_fpath})"""
                job_script = sge.gen_sge_array_job_script(
                    parse_namespace,
                    container_log_dir,
                    n_jobs,
                )
                final_script = job_script.replace('your_command_here', batch_command)
                # Submit job
                job_script_fpath = write_job_script(final_script,container_log_dir,job_script_fname)
                logger.critical(
                    f'This is the final job script that is being lauched: \n {final_script}',
                )
                cmd = f"qsub {job_script_fpath}"
                try:
                    return_code = launch_cmd(cmd)
                    logger.critical(f'\n return code of launch is {return_code} \n')
                except sp.TimeoutExpired:
                    logger.critical("❌ Sbatch submission timed out!")
                    return 1, "", "Submission timeout"
                except Exception as e:
                    logger.critical(f"❌ Error during submission: {e}")
                    return 1, "", str(e)   

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
    analysis_name = lc_config['general']['analysis_name']
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
    df_subses, num_of_jobs = do.read_df(sub_ses_list_path)
    if container in [
        'anatrois',
        'rtppreproc',
        'rtp-pipeline',
        'freesurferator',
        'rtp2-preproc',
        'rtp2-pipeline',
    ]:
        mask = (df_subses['RUN'] == 'True') & (df_subses['dwi'] == 'True')
    else:
        mask = df_subses['RUN'] == 'True'
    df_subses = df_subses.loc[mask]
    num_of_jobs = len(df_subses)
    # 3. tree sub-/ses- structure for checking
    # select the first row matching that mask
    first_row = df_subses.iloc[0]
    # extract sub and ses
    sub = first_row['sub']
    ses = first_row['ses']
    logger.critical('\n### output example subject folder structure \n')
    general_checks.cli_show_folder_struc(analysis_dir, sub, ses)

    # 4. ask for user input about folder structure and example command
    general_checks.print_option_for_review(
        num_of_jobs,
        lc_config,
        container,
        bidsdir_name,
    )

    # 5. generate the job script
    # TODO: for different containers
    # check here if the log dir and singularity home dir is being put under proper place
    # Setup log dir, create command txt under log dir
    container_log_dir = (
        f'{analysis_dir}/'
        f"job_script_dir_{datetime.now().strftime('%Y-%m-%d')}"
    )
    os.makedirs(container_log_dir, exist_ok=True)
    # 6. generate command to print
    # === Ask user to confirm before launching anything ===
    ans = input(
        'You are about to launch jobs, please review the '
        'commandline info. Continue? [y / N]: ',
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
        container_log_dir,
        run_lc
    )
    timestamp_finish = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    logger.critical(f'\n##### The finishing time is {timestamp_finish}')
    # when finished launch QC to read the log and check if everything is there
    return


# # #%%
# if __name__ == '__main__':
#     main()
