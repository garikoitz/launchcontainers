# clusters/slurm.py
from __future__ import annotations

import subprocess


def gen_slurm_opts(jobqueue_config):
    slurm_opts = {}
    slurm_opts['cpus-per-task'] = jobqueue_config['cores']
    slurm_opts['memory'] = jobqueue_config['memory']
    slurm_opts['--ntasks'] = 1
    slurm_opts['queue'] = jobqueue_config['queue']
    slurm_opts['qos'] = jobqueue_config['qos'],
    slurm_opts['time'] = jobqueue_config['walltime']
    slurm_opts['output'] = 'logfile_slurm'
    slurm_opts['error'] = 'logfile_slurm'


def run_slurm(cmds: list[str], parallel: bool = False, slurm_opts: dict = None):
    """
    Submit each cmd via `sbatch`. parallel flag is ignored (slurm is async).
    slurm_opts can include qty, mem, etc.
    """
    slurm_opts = slurm_opts or {}
    sbatch_base = ['sbatch']
    for k, v in slurm_opts.items():
        sbatch_base += [f'--{k}={v}']

    job_ids = []
    for cmd in cmds:
        full = sbatch_base + ['--wrap', cmd]
        proc = subprocess.run(full, check=True, capture_output=True, text=True)
        # parse “Submitted batch job 12345”
        job_id = proc.stdout.strip().split()[-1]
        job_ids.append(job_id)
    return job_ids
