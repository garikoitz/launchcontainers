import os
import subprocess as sp
import numpy as np
import logging
# Dask imports
from dask import delayed as delayed_dask
from dask.distributed import progress

# modules in lc
import dask_schedule_queue as dsq
import prepare_input as prepare
import utils as do


logger=logging.getLogger("GENERAL")



# %% launchcontainers

def cmdrun(host,path_to_sub_derivatives,path_to_config_json,sif_path,logfilename):
    if "BCBL" in host:
        cmd=f"singularity run -e --no-home "\
            f"--bind /bcbl:/bcbl "\
            f"--bind /tmp:/tmp "\
            f"--bind /export:/export "\
            f"--bind {path_to_sub_derivatives}/input:/flywheel/v0/input:ro "\
            f"--bind {path_to_sub_derivatives}/output:/flywheel/v0/output "\
            f"--bind {path_to_config_json}:/flywheel/v0/config.json "\
            f"{sif_path} 2>> {logfilename}.e 1>> {logfilename}.o "
    elif "DIPC" in host:
        cmd=f"singularity run -e --no-home "\
            f"--bind /scratch:/scratch "\
            f"--bind {path_to_sub_derivatives}/input:/flywheel/v0/input:ro "\
            f"--bind {path_to_sub_derivatives}/output:/flywheel/v0/output "\
            f"--bind {path_to_config_json}:/flywheel/v0/config.json "\
            f"{sif_path} 2>> {logfilename}.e 1>> {logfilename}.o "

    sp.run(cmd,shell=True)
    return cmd



def launchcontainers(lc_config, sub_ses_list, run_it,new_lc_config_path, new_sub_ses_list_path, new_container_specific_config_path):
    """
    This function launches containers generically in different Docker/Singularity HPCs
    This function is going to assume that all files are where they need to be.

    Parameters
    ----------
    sub_ses_list: Pandas data frame
        Data frame with subject information and if run or not run
    lc_config : dict
        Dictionary with all the values in the configuracion yaml file
    run_it: boolean
        used to control if we run the launchcontainer, or not
    lc_config_path: Str
        the path to lc_config file, it is the input from parser
  
   
   """
    logger.info("\n"+
                "#####################################################\n")

    host = lc_config["general"]["host"]
    jobqueue_config= lc_config["host_options"][host]
    

    basedir = lc_config["general"]["basedir"]
    container = lc_config["general"]["container"] 
    version = lc_config["container_specific"][container]["version"]
    analysis = lc_config["general"]["analysis"] 
    containerdir = lc_config["general"]["containerdir"] 
    sif_path = os.path.join(containerdir, f"{container}_{version}.sif")
    force = lc_config["general"]["force"]

    # Count how many jobs we need to launch from  sub_ses_list
    n_jobs = np.sum(sub_ses_list.RUN == "True")

    client, cluster = dsq.dask_scheduler(jobqueue_config,n_jobs)
    logger.info("---this is the cluster and client\n"
                +f"{client} \n cluster: {cluster} \n")
    
    hosts = []
    paths_to_subs_derivatives = []
    paths_to_configs_json = []
    sif_paths = []
    logfilenames = []
    future_for_print=[]
    for row in sub_ses_list.itertuples(index=True, name='Pandas'):
        sub  = row.sub
        ses  = row.ses
        RUN  = row.RUN
        dwi  = row.dwi
        func = row.func
        if RUN=="True" and dwi=="True":
            tmpdir = os.path.join(
                basedir,
                "nifti",
                "derivatives",
                f"{container}_{version}",
                "analysis-" + analysis,
                "sub-" + sub,
                "ses-" + ses,
                "output", "tmp"
            )
            logdir = os.path.join(
                basedir,
                "nifti",
                "derivatives",
                f"{container}_{version}",
                "analysis-" + analysis,
                "sub-" + sub,
                "ses-" + ses,
                "output", "log"
            )
            backup_configs = os.path.join(
                basedir,
                "nifti",
                "derivatives",
                f"{container}_{version}",
                "analysis-" + analysis,
                "sub-" + sub,
                "ses-" + ses,
                "output", "configs"
            )

            path_to_sub_derivatives=os.path.join(basedir,"nifti","derivatives",
                                                 f"{container}_{version}",
                                                 f"analysis-{analysis}",
                                                 f"sub-{sub}",
                                                 f"ses-{ses}")

            path_to_config_json=new_container_specific_config_path[0]
            path_to_config_yaml = new_lc_config_path
            path_to_subSesList = new_sub_ses_list_path


            logfilename=f"{logdir}/t-{container}_a-{analysis}_sub-{sub}_ses-{ses}"

            if not os.path.isdir(tmpdir):
                os.mkdir(tmpdir)
            if not os.path.isdir(logdir):
                os.mkdir(logdir)
            if not os.path.isdir(backup_configs):
                os.mkdir(backup_configs)
            backup_config_json = os.path.join(backup_configs, "config.json")
            backup_config_yaml = os.path.join(backup_configs, "config_lc.yaml")
            backup_subSesList = os.path.join(backup_configs, "subSesList.txt")
            
            hosts.append(host)
            paths_to_subs_derivatives.append(path_to_sub_derivatives)
            paths_to_configs_json.append(path_to_config_json)
            sif_paths.append(sif_path)
            logfilenames.append(logfilename)
            
            # this cmd is only for print the command 
            cmd= cmdrun(host,path_to_sub_derivatives,path_to_config_json,sif_path,logfilename)
            
            if run_it:                
                #future_for_print.append(delayed_dask(sp.run)(cmd,shell=True,pure=False,dask_key_name='sub-'+sub+'_ses-'+ses))
                do.copy_file(path_to_config_json,backup_config_json,force)
                do.copy_file(path_to_config_yaml, backup_config_yaml, force)
                do.copy_file(path_to_subSesList, backup_subSesList, force)
            
            else:
                logger.critical("\n"
                                +f"--------run_lc is false, if True, we would launch this command: \n"
                                +f"\n"
                                +f"{cmd}\n\n"
                                +"Please check if the job_script is properlly defined and then starting run_lc \n")
    
    if run_it:
        futures = client.map(cmdrun,hosts,paths_to_subs_derivatives,paths_to_configs_json,sif_paths,logfilenames)
        progress(futures)
        results = client.gather(futures)
        logger.info(results)
        logger.info('###########')
    
        client.close()
        cluster.close()

    return

# %% main()
def main():


    # function 1
    do.setup_logger()
    parser_namespace, parser_dict = do.get_parser()
    
    #get the path from commandline input
    lc_config_path = parser_namespace.lc_config
    lc_config = do.read_yaml(lc_config_path)
    
    sub_ses_list_path = parser_namespace.sub_ses_list
    sub_ses_list = do.read_df(sub_ses_list_path)
    
    container_specific_config_path = parser_dict["container_specific_config"]#this is a list 
    
    # stored value
    run_lc = parser_namespace.run_lc
    verbose=parser_namespace.verbose

    #set the logging level to get the command 
    
    print_command_only=lc_config["general"]["print_command_only"] #TODO this should be defiend using -v and -print command only

    # set logger message level TODO: this should be implememt to be changeable for future 
    if print_command_only:    
        logger.setLevel(logging.CRITICAL)
    
    if verbose:
        logger.setLevel(logging.INFO)    

    new_lc_config_path,new_sub_ses_list_path,new_container_specific_config_path=prepare.prepare_input_files(lc_config, lc_config_path, sub_ses_list, sub_ses_list_path, container_specific_config_path, run_lc)
    
    new_lc_config=do.read_yaml(new_lc_config_path)
    new_sub_ses_list=do.read_df(new_sub_ses_list_path)


    launchcontainers(new_lc_config, new_sub_ses_list, run_lc,  new_lc_config_path, new_sub_ses_list_path, new_container_specific_config_path)



# #%%
if __name__ == "__main__":
    main()
