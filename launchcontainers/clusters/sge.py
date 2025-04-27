from __future__ import annotations

import subprocess


def gen_sge_opts(jobqueue_config):
    sge_opts = {}
    sge_opts['q'] = jobqueue_config['queue'],
    sge_opts['l h_rt'] = jobqueue_config['walltime']
    sge_opts['s'] = '/bin/bash'
    sge_opts['o'] = 'logfile_sge'
    sge_opts['e'] = 'logfile_sge'


def run_sge(cmds: list[str], parallel: bool = False, qsub_opts: list[str] = None):
    """
    Submit each cmd via `qsub`. parallel flag is ignored (SGE is async).
    """
    qsub_opts = qsub_opts or []
    job_ids = []
    for cmd in cmds:
        full = ['qsub'] + qsub_opts + ['-b', 'y', cmd]
        proc = subprocess.run(full, check=True, capture_output=True, text=True)
        # parse the job ID from stdout (depends on your SGE setup)
        job_id = proc.stdout.strip()
        job_ids.append(job_id)
    return job_ids
