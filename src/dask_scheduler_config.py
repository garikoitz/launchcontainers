"""
MIT License

Copyright (c) 2020-2023 Garikoitz Lerma-Usabiaga
Copyright (c) 2020-2022 Mengxing Liu
Copyright (c) 2022-2024 Leandro Lecca
Copyright (c) 2022-2023 Yongning Lei
Copyright (c) 2023 David Linhardt
Copyright (c) 2023 Iñigo Tellaetxe

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
"""
import logging
import os
from dask import config
from dask.distributed import Client, LocalCluster
from dask_jobqueue import SGECluster, SLURMCluster

logger = logging.getLogger("Launchcontainers")

def initiate_cluster(jobqueue_config, optimal_n_workers, logdir):
    '''
    Parameters
    ----------
    jobqueue_config : dictionary
        read the jobquene_yaml from the yaml file
    optimal_n_workers : number of workers you give to the dask cluster

    Returns
    -------
    cluster_by_config : dask cluster object
        according to the jobquene config, we defined a cluster object we want to use.

    '''
    config.set(distributed__comm__timeouts__tcp="90s")
    config.set(distributed__comm__timeouts__connect="90s")
    config.set({"distributed.scheduler.allowed-failures": 50})
    config.set(admin__tick__limit="3h")
    if not os.path.exists(logdir):
        os.makedirs(logdir)
    
    if "sge" in jobqueue_config["manager"]:
        envextra = [f"module load {jobqueue_config['apptainer']} " 
                   #f"conda activate /export/home/tlei/tlei/conda_env/launchcontainers"
                    ]
        cluster_by_config = SGECluster(cores  = jobqueue_config["cores"], 
                                       memory = jobqueue_config["memory"],
                                       queue = jobqueue_config["queue"],
                                       name = jobqueue_config["name"],
                                       # project = jobqueue_config["project"],
                                       # processes = jobqueue_config["processes"],
                                       # interface = jobqueue_config["interface"],
                                       # nanny = None,
                                       # local_directory = jobqueue_config["local-directory"],
                                       # death_timeout = jobqueue_config["death-timeout"],
                                       # worker_extra_args = None,
                                       job_script_prologue = envextra,
                                       # job_script_prologue = None,
                                       # header_skip=None,
                                       # job_directives_skip=None,
                                       log_directory=logdir,
                                       # shebang=jobqueue_config["shebang"],
                                       # python=None,
                                       # config_name=None,
                                       # n_workers=optimal_n_workers,
                                       # silence_logs=None,
                                       # asynchronous=None,
                                       # security=None,
                                       # scheduler_options=None,
                                       # scheduler_cls=None,
                                       # shared_temp_directory=None,
                                       # resource_spec=jobqueue_config["resource-spec"],
                                       walltime=jobqueue_config["walltime"])#,
                                       #job_extra_directives=job_extra_directives)
        cluster_by_config.scale(jobs=optimal_n_workers)

    elif "slurm" in jobqueue_config["manager"]:
        envextra = [f"module load {jobqueue_config['apptainer']} ",\
                    f"export SINGULARITYENV_TMPDIR={jobqueue_config['tmpdir']}",\
                    "export SINGULARITY_BIND=''",\
                    "TMPDIR="]
        cluster_by_config = SLURMCluster(cores = jobqueue_config["cores"], 
                                         memory = jobqueue_config["memory"],
                                         job_script_prologue = envextra,
                                         log_directory = logdir,
                                         queue = jobqueue_config["queue"],
                                         name = jobqueue_config["name"],
                                         death_timeout = 300,#jobqueue_config["death-timeout"],
                                         walltime=jobqueue_config["walltime"],
                                         job_extra_directives = ["--export=ALL"])
        cluster_by_config.scale(jobs=optimal_n_workers)
    elif "local" in jobqueue_config["manager"]:
        launch_mode=jobqueue_config['launch_mode']
        if launch_mode=="parallel":
            logger.debug("defining local cluster in parallel mode")
            cluster_by_config = LocalCluster(  
                processes = jobqueue_config["processes"],       
                n_workers = optimal_n_workers,
                threads_per_worker = jobqueue_config["threads_per_worker"],
                memory_limit = jobqueue_config["memory_limit"],
            )
         
    else:
        logger.error(
            "dask configuration wasn't detected, "
            "if you are using a cluster please look at "
            "the jobqueue YAML example, modify it so it works in your cluster "
            "and add it to ~/.config/dask "
            "local configuration will be used."
            "You can find a jobqueue YAML example in the pySPFM/jobqueue.yaml file."
        )
        cluster_by_config = None
    return cluster_by_config


def dask_scheduler(jobqueue_config, optimal_n_workers, logdir):
    
    if jobqueue_config is None:
        logger.warning(
            "dask configuration wasn't detected, "
            "if you are using a cluster please look at "
            "the jobqueue YAML example, modify it so it works in your cluster "
            "and add it to ~/.config/dask "
            "local configuration will be used."
            
        )
        cluster = None
    else:
        if not "local" in jobqueue_config["manager"]:
            cluster = initiate_cluster(jobqueue_config, optimal_n_workers, logdir)
        
        elif "local" in jobqueue_config["manager"]:
            launch_mode=jobqueue_config['launch_mode']
            if not launch_mode=="serial":
                cluster = initiate_cluster(jobqueue_config, optimal_n_workers, logdir)
                 
    client = None if cluster is None else Client(cluster)
   
    return client, cluster
