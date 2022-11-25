import logging
from os.path import expanduser, join

import yaml
from dask import config
from dask.distributed import Client
from dask_jobqueue import PBSCluster, SGECluster, SLURMCluster

def dask_scheduler(jobs):
    """
    Checks if the user has a dask_jobqueue configuration file, and if so,
    returns the appropriate scheduler according to the file parameters
    """
    # look if default ~ .config/dask/jobqueue.yaml exists
    with open(join(expanduser("~"), ".config/dask/jobqueue.yaml"), "r") as stream:
        data = yaml.load(stream, Loader=yaml.FullLoader)

    if data is None:
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
        cluster = initiate_cluster(data, jobs)
    client = None if cluster is None else Client(cluster)
    return client, cluster


def initiate_cluster(data, jobs):
    config.set(distributed__comm__timeouts__tcp="90s")
    config.set(distributed__comm__timeouts__connect="90s")
    config.set(scheduler="single-threaded")
    config.set({"distributed.scheduler.allowed-failures": 50})
    config.set(admin__tick__limit="3h")
    if "sge" in data["jobqueue"]:
        result = SGECluster()
        result.scale(jobs)
    elif "pbs" in data["jobqueue"]:
        result = PBSCluster()
        result.scale(jobs)
    elif "slurm" in data["jobqueue"]:
        result = SLURMCluster()
        result.scale(jobs)
    else:
        LGR.warning(
            "dask configuration wasn't detected, "
            "if you are using a cluster please look at "
            "the jobqueue YAML example, modify it so it works in your cluster "
            "and add it to ~/.config/dask "
            "local configuration will be used."
            "You can find a jobqueue YAML example in the pySPFM/jobqueue.yaml file."
        )
        result = None
    return result