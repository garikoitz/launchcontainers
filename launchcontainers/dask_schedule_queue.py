import logging
from os.path import expanduser, join

import yaml
from dask import config
from dask.distributed import Client
from dask_jobqueue import PBSCluster, SGECluster, SLURMCluster
LGR = logging.getLogger("GENERAL")


def initiate_cluster(jobqueue_config, n_job):
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
    config.set(distributed__comm__timeouts__tcp="90s")
    config.set(distributed__comm__timeouts__connect="90s")
    config.set(scheduler="single-threaded")
    config.set({"distributed.scheduler.allowed-failures": 50})
    config.set(admin__tick__limit="3h")
    
    if "sge" in jobqueue_config["manager"]:
        cluster_by_config = SGECluster(cores  = jobqueue_config["cores"], 
                                       memory = jobqueue_config["memory"],
                                       queue = jobqueue_config["queue"],
                                       project = jobqueue_config["project"],
                                       processes = jobqueue_config["processes"],
                                       interface = jobqueue_config["interface"],
                                       # nanny = None,
                                       local_directory = jobqueue_config["local-directory"],
                                       death_timeout = jobqueue_config["death-timeout"],
                                       # worker_extra_args = None,
                                       env_extra = jobqueue_config["env-extra"],
                                       # job_script_prologue = None,
                                       # header_skip=None,
                                       # job_directives_skip=None,
                                       log_directory=jobqueue_config["log-directory"],
                                       shebang=jobqueue_config["shebang"],
                                       # python=None,
                                       # config_name=None,
                                       name=jobqueue_config["name"],
                                       # n_workers=None,
                                       # silence_logs=None,
                                       # asynchronous=None,
                                       # security=None,
                                       # scheduler_options=None,
                                       # scheduler_cls=None,
                                       # shared_temp_directory=None,
                                       resource_spec=jobqueue_config["resource-spec"],
                                       walltime=jobqueue_config["walltime"],
                                       job_extra_directives=jobqueue_config["job-extra"])
        cluster_by_config.scale(n_job)

    elif "pbs" in jobqueue_config["manager"]:
        cluster_by_config = PBSCluster(cores = core, memory = memory)
        cluster_by_config.scale(n_job)
    elif "slurm" in jobqueue_config["manager"]:
        cluster_by_config = SLURMCluster(cores = core, memory = memory)
        cluster_by_config.scale(n_job)
    else:
        LGR.warning(
            "dask configuration wasn't detected, "
            "if you are using a cluster please look at "
            "the jobqueue YAML example, modify it so it works in your cluster "
            "and add it to ~/.config/dask "
            "local configuration will be used."
            "You can find a jobqueue YAML example in the pySPFM/jobqueue.yaml file."
        )
        cluster_by_config = None
    return cluster_by_config


def dask_scheduler(jobqueue_config ,n_job):

    if jobqueue_config is None:
        LGR.warning(
            "dask configuration wasn't detected, "
            "if you are using a cluster please look at "
            "the jobqueue YAML example, modify it so it works in your cluster "
            "and add it to ~/.config/dask "
            "local configuration will be used."
            "You can find a jobqueue YAML example in the pySPFM/jobqueue.yaml file."
        )
        cluster = None
    else:
        cluster = initiate_cluster(jobqueue_config, n_job)
    client = None if cluster is None else Client(cluster)
    return client, cluster






