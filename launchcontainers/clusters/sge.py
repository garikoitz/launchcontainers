# clusters/slurm.py
from __future__ import annotations

import os.path as op

from launchcontainers import utils as do


def gen_sge_array_job_script(
    parse_namespace,
    log_dir,
    n_jobs,

):
    """
    Alternative implementation using SLURM array jobs (more efficient).

    Args:
        Same as gen_slurm_job_script

    Returns:
        Single job ID for the array job (None if dry_run=True)
    """
    # read LC config yml from analysis dir
    analysis_dir = parse_namespace.workdir
    lc_config_fpath = op.join(analysis_dir, 'lc_config.yaml')
    lc_config = do.read_yaml(lc_config_fpath)
    host = lc_config['general']['host']
    jobqueue_config = lc_config['host_options'][host]
    # below is the job specific configs
    job_name = jobqueue_config['job_name']
    queue = jobqueue_config['queue']
    walltime = jobqueue_config['walltime']

    # Generate array job script
    job_name = f'{job_name}_array'
    job_script = f"""#!/bin/bash
    #$ -t 1-{len(n_jobs)}
    #$ -N {job_name}
    #$ -o {log_dir}/{job_name}_$JOB_ID_$TASK_ID.out
    #$ -e {log_dir}/{job_name}_$JOB_ID_$TASK_ID.err
    #$ -l h_rt={walltime}
    #$ -S /bin/bash
    #$ -q {queue}

    LOG_DIR={log_dir}
    echo "Starting array task $SGE_TASK_ID on $(hostname)"
    echo "Job ID: $JOB_ID"

    # Read the command for this array index

    COMMAND="your_command_here"
    echo "Executing: $COMMAND"
    eval $COMMAND

    exitcode=$?

    echo "Task $SGE_TASK_ID completed with exit code $exitcode"

    # Output results to a TSV
    echo "Task $SGE_TASK_ID   Exit code: $exitcode" >> $LOG_DIR/{job_name}_$JOB_ID.tsv

    exit $exitcode

    """

    return job_script
