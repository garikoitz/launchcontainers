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

from dask import config
from dask.distributed import Client
from dask.distributed import LocalCluster
from dask_jobqueue import SGECluster
from dask_jobqueue import SLURMCluster
logger = logging.getLogger('Launchcontainers')


def initiate_cluster(jobqueue_config, n_job, dask_logdir):
    '''
    Parameters
    ----------
    jobqueue_config : dictionary
        read the jobquene_yaml from the yaml file
    n_job : not clear what should it be
        basically it's a quene specific thing, needs to check if it's dask specific.

    Returns
    -------
    cluster_by_config : dask cluster object
        according to the jobquene config, we defined a cluster object we want to use.

    '''
    config.set(distributed__comm__timeouts__tcp='90s')
    config.set(distributed__comm__timeouts__connect='90s')
    config.set(scheduler='single-threaded')
    config.set({'distributed.scheduler.allowed-failures': 50})
    config.set(admin__tick__limit='3h')

    if 'sge' in jobqueue_config['manager']:
        # envextra is needed for launch jobs on SGE and SLURM
        envextra = [
            f"module load {jobqueue_config['apptainer']} ",
        ]
        cluster_by_config = SGECluster(
            queue=jobqueue_config['queue'],
            cores=jobqueue_config['cores'],
            memory=jobqueue_config['memory'],
            walltime=jobqueue_config['walltime'],
            log_directory=dask_logdir,
            job_script_prologue=envextra,
        )
        cluster_by_config.scale(jobs=n_job)

    elif 'slurm' in jobqueue_config['manager']:
        envextra = [
            f"module load {jobqueue_config['apptainer']} "
            f"export SINGULARITYENV_TMPDIR={jobqueue_config['tmpdir']}",
            "export SINGULARITY_BIND=''",
        ]
        cluster_by_config = SLURMCluster(
            cores=jobqueue_config['cores'],
            memory=jobqueue_config['memory'],
            log_directory=dask_logdir,
            queue=jobqueue_config['queue'],
            job_extra_directives=['--export=ALL'] + jobqueue_config['job_extra_directives'],
            death_timeout=300,
            walltime=jobqueue_config['walltime'],
            job_script_prologue=envextra,
        )
        cluster_by_config.scale(jobs=n_job)

    elif 'local' in jobqueue_config['manager']:
        cluster_by_config = LocalCluster(
            processes=False,
            n_workers=n_job,
            threads_per_worker=jobqueue_config['threads_per_worker'],
            memory_limit=jobqueue_config['memory_limit'],
        )

    else:
        logger.warning(
            "dask configuration wasn't detected, "
            'if you are using a cluster please look at '
            'the jobqueue YAML example, modify it so it works in your cluster '
            'and add it to ~/.config/dask '
            'local configuration will be used.'
            'You can find a jobqueue YAML example in the pySPFM/jobqueue.yaml file.',
        )
        cluster_by_config = None

    return cluster_by_config


def dask_scheduler(jobqueue_config, n_job, dask_logdir):
    if jobqueue_config is None:
        logger.warning(
            "dask configuration wasn't detected, "
            'if you are using a cluster please look at '
            'the jobqueue YAML example, modify it so it works in your cluster '
            'and add it to ~/.config/dask '
            'local configuration will be used.',

        )
        cluster = None
    else:
        cluster = initiate_cluster(jobqueue_config, n_job, dask_logdir)

    client = None if cluster is None else Client(cluster)

    return client, cluster
