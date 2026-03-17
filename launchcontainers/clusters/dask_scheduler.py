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

import subprocess as sp

from dask import config
from dask.distributed import Client
from dask.distributed import LocalCluster
from dask_jobqueue import SGECluster
from dask_jobqueue import SLURMCluster
from launchcontainers.log_setup import console


def initiate_cluster(jobqueue_config, n_job, dask_logdir):
    """
    Create and scale a Dask cluster from the configured scheduler backend.

    Parameters
    ----------
    jobqueue_config : dict
        Scheduler-specific settings loaded from the launchcontainers YAML file.
    n_job : int
        Number of worker jobs or local workers to request.
    dask_logdir : str
        Directory where Dask worker logs should be written.

    Returns
    -------
    dask_jobqueue.core.JobQueueCluster or dask.distributed.LocalCluster or None
        Configured cluster object for the requested backend.
    """
    config.set(distributed__comm__timeouts__tcp="90s")
    config.set(distributed__comm__timeouts__connect="90s")
    config.set(scheduler="single-threaded")
    config.set({"distributed.scheduler.allowed-failures": 50})
    config.set(admin__tick__limit="3h")
    console.print(f"\n $$$$$ dask number of jobs scaled is {n_job}", style="bold red")
    if "sge" in jobqueue_config["manager"]:
        # envextra is needed for launch jobs on SGE and SLURM
        envextra = [
            f"module load {jobqueue_config['apptainer']} ",
        ]
        cluster_by_config = SGECluster(
            queue=jobqueue_config["queue"],
            cores=jobqueue_config["cores"],
            memory=jobqueue_config["memory"],
            walltime=jobqueue_config["walltime"],
            processes=1,
            log_directory=dask_logdir,
            job_script_prologue=envextra,
        )
        cluster_by_config.scale(jobs=n_job)

    elif "slurm" in jobqueue_config["manager"]:
        envextra = [
            f"module load {jobqueue_config['apptainer']} ",
            f"export SINGULARITYENV_TMPDIR={jobqueue_config['tmpdir']}",
            "export SINGULARITY_BIND=''",
        ]
        cluster_by_config = SLURMCluster(
            cores=jobqueue_config["cores"],
            memory=jobqueue_config["memory"],
            processes=1,
            log_directory=dask_logdir,
            queue=jobqueue_config["queue"],
            job_extra_directives=["--export=ALL"]
            + jobqueue_config["job_extra_directives"],
            death_timeout=300,
            walltime=jobqueue_config["walltime"],
            job_script_prologue=envextra,
        )
        cluster_by_config.scale(jobs=n_job)

    elif "local" in jobqueue_config["manager"]:
        cluster_by_config = LocalCluster(
            processes=False,
            n_workers=n_job,
            threads_per_worker=jobqueue_config["threads_per_worker"],
            memory_limit=jobqueue_config["memory_limit"],
        )

    else:
        console.print(
            "dask configuration wasn't detected, "
            "if you are using a cluster please look at "
            "the jobqueue YAML example, modify it so it works in your cluster "
            "and add it to ~/.config/dask "
            "local configuration will be used."
            "You can find a jobqueue YAML example in the pySPFM/jobqueue.yaml file.",
            style="yellow",
        )
        cluster_by_config = None

    return cluster_by_config


def dask_scheduler(jobqueue_config, n_job, dask_logdir):
    """
    Create a Dask client and cluster pair.

    Parameters
    ----------
    jobqueue_config : dict
        Scheduler-specific settings loaded from the launchcontainers YAML file.
    n_job : int
        Number of worker jobs or local workers to request.
    dask_logdir : str
        Directory where Dask worker logs should be written.

    Returns
    -------
    tuple[dask.distributed.Client | None, object | None]
        The connected client and the cluster object.
    """
    if jobqueue_config is None:
        console.print(
            "dask configuration wasn't detected, "
            "if you are using a cluster please look at "
            "the jobqueue YAML example, modify it so it works in your cluster "
            "and add it to ~/.config/dask "
            "local configuration will be used.",
            style="yellow",
        )
        cluster = None
    else:
        cluster = initiate_cluster(jobqueue_config, n_job, dask_logdir)

    client = None if cluster is None else Client(cluster)

    return client, cluster


def print_job_script(host, jobqueue_config, n_jobs, daskworker_logdir):
    """
    Print the scheduler script that Dask would submit for the current backend.

    Parameters
    ----------
    host : str
        Host name from the launchcontainers configuration.
    jobqueue_config : dict
        Scheduler-specific settings loaded from the YAML file.
    n_jobs : int
        Number of worker jobs to request.
    daskworker_logdir : str
        Directory where Dask worker logs should be written.
    """
    if host == "local":
        launch_mode = jobqueue_config["launch_mode"]
    # If the host is not local, print the job script to be launched in the cluster.
    if host != "local" or (host == "local" and launch_mode == "dask_worker"):
        _, cluster = dask_scheduler(jobqueue_config, n_jobs, daskworker_logdir)
        if host != "local":
            console.print(
                f"Cluster job script for this command is:\n{cluster.job_script()}",  # type: ignore
                style="bold red",
            )
        elif host == "local" and launch_mode == "dask_worker":
            console.print(f"Local job script by dask is:\n{cluster}", style="bold red")
        else:
            console.print("Job launched on local, no job script", style="bold red")
    return


def launch_with_dask(jobqueue_config, n_jobs, daskworker_logdir, cmds):
    """
    Execute a list of shell commands through a Dask cluster.

    Parameters
    ----------
    jobqueue_config : dict
        Scheduler-specific settings loaded from the YAML file.
    n_jobs : int
        Number of worker jobs to request.
    daskworker_logdir : str
        Directory where Dask worker logs should be written.
    cmds : list[str]
        Shell commands to run in parallel.
    """

    def run_cmd(cmd: str):
        return sp.run(cmd, shell=True).returncode

    client, cluster = dask_scheduler(jobqueue_config, n_jobs, daskworker_logdir)
    console.print(
        "---this is the cluster and client\n" + f"{client} \n cluster: {cluster} \n",
        style="cyan",
    )

    # Compose the command to run in the cluster
    futures = client.map(  # type: ignore
        run_cmd,
        cmds,
    )
    results = client.gather(futures)  # type: ignore
    console.print(results, style="cyan")
    console.print("###########", style="cyan")
    # Close the connection with the client and the cluster, and inform about it
    client.close()  # type: ignore
    cluster.close()  # type: ignore

    console.print(
        "\n" + "launchcontainer finished, all the jobs are done", style="bold red"
    )
    return
