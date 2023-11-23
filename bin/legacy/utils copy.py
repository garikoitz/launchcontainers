import logging
from os.path import expanduser, join

import yaml
from dask import config
from dask.distributed import Client
from dask_jobqueue import PBSCluster, SGECluster, SLURMCluster
LGR = logging.getLogger("GENERAL")


def initiate_cluster(jobqueue_yaml, jobs):
    '''
    

    Parameters
    ----------
    jobqueue_yaml : dictionary
        read the jobquene_yaml from the yaml file
    jobs : not clear what should it be
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
    
    if "sge" in jobqueue_yaml["jobqueue"]:
        cluster_by_config = SGECluster()
        cluster_by_config.scale(jobs)
    elif "pbs" in jobqueue_yaml["jobqueue"]:
        cluster_by_config = PBSCluster()
        cluster_by_config.scale(jobs)
    elif "slurm" in jobqueue_yaml["jobqueue"]:
        cluster_by_config = SLURMCluster()
        cluster_by_config.scale(jobs)
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


def dask_scheduler(jobs):
    
    """
    Checks if the user has a dask_jobqueue configuration file, and if so,
    returns the appropriate scheduler according to the file parameters
    """
    # look if default ~ .config/dask/jobqueue.yaml exists
    with open(join(expanduser("~"), ".config/dask/jobqueue.yaml"), "r") as stream:
        jobqueue_yaml = yaml.load(stream, Loader=yaml.FullLoader)

    if jobqueue_yaml is None:
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
        cluster = initiate_cluster(jobqueue_yaml, jobs)
    client = None if cluster is None else Client(cluster)
    return client, cluster