"""
MIT License

Copyright (c) 2020-2023 Garikoitz Lerma-Usabiaga
Copyright (c) 2020-2022 Mengxing Liu
Copyright (c) 2022-2023 Leandro Lecca
Copyright (c) 2022-2023 Yongning Lei
Copyright (c) 2023 David Linhardt
Copyright (c) 2023 Iñigo Tellaetxe

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
"""
import os
import subprocess as sp
import numpy as np
import logging
# modules in lc

from bids import BIDSLayout
from dask.distributed import progress

from prepare_inputs import dask_scheduler_config as dsq
from prepare_inputs import prepare as prepare
from prepare_inputs import utils as do

logger=logging.getLogger("GENERAL")


# %% launchcontainers
def generate_cmd(lc_config,sub,ses,dir_analysis, lst_container_specific_configs,run_lc):
    """Puts together the command to send to the container.

    Args:
        lc_config (str): _description_
        sub (str): _description_
        ses (str): _description_
        dir_analysis (str): _description_
        lst_container_specific_configs (list): _description_
        run_lc (str): _description_

    Raises:
        ValueError: Raised in presence of a faulty config.yaml file, or when the formed command is not recognized.

    Returns:
        _type_: _description_
    """
    
    # Relevant directories
    basedir=lc_config['general']['basedir']  # All other relevant directories stem from this one
    homedir= os.path.join(basedir,'singularity_home')
    container= lc_config['general']['container']
    host = lc_config['general']['host']
    containerdir=  lc_config['general']['containerdir']  
    
    # Information relevant to the host and container
    jobqueue_config= lc_config['host_options'][host]
    version = lc_config["container_specific"][container]["version"]
    use_module= jobqueue_config['use_module']
    bind_options= jobqueue_config['bind_options']

    # Location of the Singularity Image File (.sif)
    container_name= os.path.join(containerdir, f'{container}_{version}.sif')
    # Define the directory and the file name to output the log of each subject
    logdir= os.path.join(
            dir_analysis,
            "sub-" + sub,
            "ses-" + ses,
            "output", "log"
        )
    logfilename=f"{logdir}/t-{container}-sub-{sub}_ses-{ses}"
    
    path_to_sub_derivatives=os.path.join(dir_analysis,
                                    f"sub-{sub}",
                                    f"ses-{ses}")
    
    if container in ['anatrois', 'rtppreproc','rtp-pipeline']:  
        logger.info("\n"+ f'start to generate the DWI PIPELINE command')
        config_json= lst_container_specific_configs[0]
        logger.debug(f"\n the sub is {sub} \n the ses is {ses} \n the analysis dir is {dir_analysis}")
        
        bind_cmd=''
        for bind in bind_options:
            bind_cmd += f"--bind {bind}:{bind} "           
        
        env_cmd=''
        if host == "local":
            if use_module == True:
                env_cmd= f"module load {jobqueue_config['apptainer']} &&"

        cmd=f"{env_cmd} singularity run -e --no-home {bind_cmd}"\
            f"--bind {path_to_sub_derivatives}/input:/flywheel/v0/input:ro "\
            f"--bind {path_to_sub_derivatives}/output:/flywheel/v0/output "\
            f"--bind {config_json}:/flywheel/v0/config.json "\
            f"{container_name} 2>> {logfilename}.e 1>> {logfilename}.o "
        
    
    # Check which container we are using, and define the command accordingly
    if container == 'fmriprep':
        logger.info("\n"+ f'start to generate the FMRIPREP command')
                
        nthreads=lc_config["container_specific"][container]['nthreads']
        mem=lc_config["container_specific"][container]['mem'] 
        fs_license=lc_config["container_specific"][container]['fs_license']
        containerdir=  lc_config['general']['containerdir']  
        container_path= os.path.join(containerdir, f"{container}_{lc_config['container_specific'][container]['version']}.sif")
        precommand=f'mkdir -p {homedir}; '\
                f'unset PYTHONPATH; ' \
        
        if "local" == host:
            cmd =   precommand+f'singularity run '\
                    f'-H {homedir} '\
                    f'-B {basedir}:/base -B {fs_license}:/license '\
                    f'--cleanenv {container_path} '\
                    f'-w {dir_analysis} '\
                    f'/base/BIDS {dir_analysis} participant '\
                        f'--participant-label sub-{sub} '\
                    f'--skip-bids-validation '\
                    f'--output-spaces func fsnative fsaverage T1w MNI152NLin2009cAsym '\
                    f'--dummy-scans 0 '\
                    f'--use-syn-sdc '\
                    f'--fs-license-file /license/license.txt '\
                    f'--nthreads {nthreads} '\
                    f'--omp-nthreads {nthreads} '\
                    f'--stop-on-first-crash '\
                    f'--mem_mb {(mem*1000)-5000} '\
        
        if host in ['BCBL','DIPC']:
            cmd = precommand+f'singularity run '\
                    f'-H {homedir} '\
                    f'-B {basedir}:/base -B {fs_license}:/license '\
                    f'--cleanenv {container_path} '\
                    f'-w {dir_analysis} '\
                    f'/base/BIDS {dir_analysis} participant '\
                        f'--participant-label sub-{sub} '\
                    f'--skip-bids-validation '\
                    f'--output-spaces func fsnative fsaverage T1w MNI152NLin2009cAsym '\
                    f'--dummy-scans 0 '\
                    f'--use-syn-sdc '\
                    f'--fs-license-file /license/license.txt '\
                    f'--nthreads {nthreads} '\
                    f'--omp-nthreads {nthreads} '\
                    f'--stop-on-first-crash '\
                    f'--mem_mb {(mem*1000)-5000} '\
    
    
    if container in ['prfprepare','prfreport','prfanalyze-vista']:
        config_name=lc_config['container_specific'][container]['config_name']
        homedir= os.path.join(basedir,'singularity_home')
        container_path= os.path.join(containerdir, f"{container}_{lc_config['container_specific'][container]['version']}.sif")
        if host in ['BCBL','DIPC']:
            cmd=   'unset PYTHONPATH; '\
                f'singularity run '\
                f'-H {homedir} '\
                f'-B {basedir}/derivatives/fmriprep:/flywheel/v0/input '\
                f'-B {dir_analysis}:/flywheel/v0/output '\
                f'-B {basedir}/BIDS:/flywheel/v0/BIDS '\
                    f'-B {dir_analysis}/{config_name}.json:/flywheel/v0/config.json '\
	            f'-B {basedir}/license/license.txt:/opt/freesurfer/.license '\
                f'--cleanenv {container_path} '\
        
        elif host == 'local':
            cmd= 'unset PYTHONPATH; '\
                f'singularity run '\
                f'-H {homedir} '\
                f'-B {basedir}/derivatives/fmriprep:/flywheel/v0/input '\
                f'-B {dir_analysis}:/flywheel/v0/output '\
                f'-B {basedir}/BIDS:/flywheel/v0/BIDS '\
                    f'-B {dir_analysis}/{config_name}.json:/flywheel/v0/config.json '\
	            f'-B {basedir}/license/license.txt:/opt/freesurfer/.license '\
                f'--cleanenv {container_path} '\

    # If after all configuration, we do not have command, raise an error
    if cmd is None:
        logger.error("\n"+ f'the DWI PIPELINE command is not assigned, please check your config.yaml[general][host] session\n')
        raise ValueError('cmd is not defined, aborting')
    if run_lc:
        sp.run(cmd, shell=True)
    
    return cmd

#%% the launchcontainer
def launchcontainer(dir_analysis, lc_config, sub_ses_list, parser_namespace, path_to_analysis_container_specific_config):
    """
    This function launches containers generically in different Docker/Singularity HPCs
    This function is going to assume that all files are where they need to be.

    Args:
        dir_analysis (str): _description_
        lc_config (str): path to launchcontainer config.yaml file
        sub_ses_list (_type_): parsed CSV containing the subject list to be analyzed, and the analysis options
        parser_namespace (argparse.Namespace): command line arguments 
    """
    logger.info("\n"+
                "#####################################################\n")

    # Get the host and jobqueue config info from the config.yaml file
    host = lc_config["general"]["host"]
    jobqueue_config= lc_config["host_options"][host]
    logger.debug(f'\n,, this is the job_queue config {jobqueue_config}')
    
    force = lc_config["general"]["force"]    
    logdir= os.path.join(
                dir_analysis,"daskworker_log"
            )

    # Count how many jobs we need to launch from  sub_ses_list
    n_jobs = np.sum(sub_ses_list.RUN == "True")

    client, cluster = dsq.dask_scheduler(jobqueue_config,n_jobs, logdir)
    
    logger.info("---this is the cluster and client\n"
                +f"{client} \n cluster: {cluster} \n")

    run_lc= parser_namespace.run_lc

    lc_configs=[]
    subs=[]
    sess=[]
    dir_analysiss=[]
    paths_to_analysis_config_json=[]
    run_lcs=[]
    

    # PREPARATION mode
    if not run_lc:
        logger.critical(f"\nlaunchcontainers.py was run in PREPARATION mode (without option --run_lc)\n" \
                            f"Please check that: \n" \
                            f"    (1) launchcontainers.py prepared the input data properly\n" \
                            f"    (2) the command created for each subject is properly formed\n" \
                            f"         (you can copy the command for one subject and launch it " \
                                      f"on the prompt before you launch multiple subjects\n" \
                            f"    (3) Once the check is done, launch the jobs by adding --run_lc to the first command you executed.\n"
        )
        # If the host is not local, print the job script to be launched in the cluster.
        if host != 'local':
            logger.critical(
                        f"The cluster job script for this command is:\n" \
                        f"{cluster.job_script()}"
                        )
    # Iterate over the provided subject list
    for row in sub_ses_list.itertuples(index=True, name='Pandas'):
        sub  = row.sub
        ses  = row.ses
        RUN  = row.RUN
        dwi  = row.dwi
        
        if RUN=="True":
            # Append config, subject, session, and path info in corresponding lists
            lc_configs.append(lc_config)
            subs.append(sub)
            sess.append(ses)
            dir_analysiss.append(dir_analysis)
            paths_to_analysis_config_json.append(path_to_analysis_container_specific_config)
            run_lcs.append(run_lc)    

            if not run_lc:
                # This cmd is only for print the command 
                command= generate_cmd(lc_config,sub,ses,dir_analysis, path_to_analysis_container_specific_config, run_lc)
                logger.critical(f"\nCOMMAND for subject-{sub}, and session-{ses}:\n" \
                                f"{command}\n\n"
                                )
                if lc_config['general']['container']=='fmriprep':
                    logger.critical('\n'+ 'fmriprep now can not deal with session specification, so the analysis are running on all sessions of the subject you are specifying')
        
    # RUN mode
    if run_lc:  
        # Compose the command to run in the cluster  
        futures = client.map(generate_cmd,lc_configs,subs,sess,dir_analysiss,paths_to_analysis_config_json, run_lcs)
        # Record the progress
        progress(futures)
        # Get the info and report it in the logger
        results = client.gather(futures)
        logger.info(results)
        logger.info('###########')
        # Close the connection with the client and the cluster, and inform about it
        client.close()
        cluster.close()
        
        logger.critical("\n"
                        + "launchcontainer finished, all the jobs are done")
    
    return
# %% main()
def main():
    # Set the logging level to get the command
    do.setup_logger() 
    
    # Get the path from command line input
    parser_namespace = do.get_parser()
    lc_config_path = parser_namespace.lc_config
    lc_config = do.read_yaml(lc_config_path)   
    
    # Get general information from the config.yaml file
    container= lc_config['general']['container']
    basedir=lc_config['general']['basedir'] 
    bidsdir_name= lc_config['general']['bidsdir_name'] 
    sub_ses_list_path = parser_namespace.sub_ses_list
    sub_ses_list = do.read_df(sub_ses_list_path)    
    
    # Stored value
    verbose=parser_namespace.verbose
    debug= parser_namespace.DEBUG
    
    # Set the verbosity level
    print_command_only=lc_config["general"]["print_command_only"]  # TODO: this should be defined using -v and -print command only
    
    # Set logger message level 
    # TODO: this implementation should allow changing the level of verbosity in the three levels 
    if print_command_only:    
        logger.setLevel(logging.CRITICAL)
    if verbose:
        logger.setLevel(logging.INFO)    
    if debug:
        logger.setLevel(logging.DEBUG)
    logger.critical('start reading the BIDS layout')
    # Prepare file and launch containers
    # First of all prepare the analysis folder: it create you the analysis folder automatically so that you are not messing up with different analysis
    dir_analysis, path_to_analysis_container_specific_config = prepare.prepare_analysis_folder(parser_namespace, lc_config)
    layout= BIDSLayout(os.path.join(basedir,bidsdir_name))
    
    
    # Prepare mode
    if container in ['anatrois', 'rtppreproc', 'rtp-pipeline']:  # TODO: define list in another module for reusability accross modules and functions
        prepare.prepare_dwi_input(parser_namespace, dir_analysis, lc_config, sub_ses_list, layout)
    
    if container == 'fmriprep':
        prepare.fmrprep_intended_for(sub_ses_list, layout)
    
    if container in ['prfprepare', 'prfanalyze-vista', 'prfreport']:
        logger.info(f"Container not implemented yet.")
        pass
    
    # Run mode
    launchcontainer(dir_analysis, lc_config, sub_ses_list, parser_namespace, path_to_analysis_container_specific_config)
    

    
# #%%
if __name__ == "__main__":
    main()
