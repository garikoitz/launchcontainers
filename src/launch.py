# -*- coding: utf-8 -*-
"""
MIT License

Copyright (c) 2020-2024 Garikoitz Lerma-Usabiaga
Copyright (c) 2020-2022 Mengxing Liu
Copyright (c) 2022-2024 Leandro Lecca
Copyright (c) 2022-2024 Yongning Lei
Copyright (c) 2023 David Linhardt
Copyright (c) 2023 Iñigo Tellaetxe

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
"""
import os
import os.path as op
import logging
import dask
from bids import BIDSLayout
import dask_scheduler_config as config_dask
import utils as do
import generate_command as gen_cmd
from prepare_inputs import prepare
import subprocess
from py_pipeline import l1_glm



logger = logging.getLogger("Launchcontainers")

def prepare_dask_futures(
    ananlysis_dir,
    lc_config,
    sub_ses_list,
    dict_store_cs_configs
):
    """
    This function have 2 function
    1. prepare the command and print it
    2. append the command into a list for dask to gather them and launch them

    Args:
        ananlysis_dir (str): _description_
        lc_config (str): path to launchcontainer config.yaml file
        sub_ses_list (_type_): parsed CSV containing the subject list to be analyzed, and the analysis options
        parser_namespace (argparse.Namespace): command line arguments
    """
    logger.info("\n" + "#####################################################\n")

    # Get the host and jobqueue config info from the config.yaml file
    container=lc_config["general"]["container"]
    containerdir=lc_config["general"]["containerdir"]
    host = lc_config["general"]["host"]
    jobqueue_config = lc_config["host_options"][host]
    logger.debug(f"\n This is the job_queue config {jobqueue_config}")
    logdir = os.path.join(ananlysis_dir, "daskworker_log")
    launch_mode=jobqueue_config['launch_mode']
    # Count how many jobs we need to launch from  sub_ses_list
    n_jobs = sub_ses_list.shape[0]
    # n_worker should be constrained by n_cores you have in total and core_per_worker
    # add a check here
    total_core_avail=jobqueue_config["n_cores"]
    threads_per_worker=jobqueue_config["threads_per_worker"]
    # Calculate the optimal number of workers
    optimal_n_workers = min(total_core_avail // threads_per_worker, n_jobs)
    
    # Echo the command
    # if in local and you want to do it serially
    # you don't need set up dask LocalCluster or dask Client, you just set up the scheduler to synchronous and use dask.compute() to compute the job
    # import dask dask.config.set(scheduler='synchronous')  # overwrite default with single-threaded scheduler
    # if in local and you want to do it parallel, you need to consider total cores and also you number of jobs
    # if in SGE or SLURM, you will never do it in serial, so you will use job-queue to set up your job scripts 
    logger.critical(
        f"\n Launchcontainers.py was run in PREPARATION mode (without option --run_lc)\n"
        f"Please check that: \n"
        f"    (1) launchcontainers.py prepared the input data properly\n"
        f"    (2) the command created for each subject is properly formed\n"
        f"         (you can copy the command for one subject and launch it "
        f"on the prompt before you launch multiple subjects\n"
        f"    (3) Once the check is done, launch the jobs by adding --run_lc to the first command you executed.\n"
    )
    launch_mode=jobqueue_config['launch_mode']
    if "local" in jobqueue_config["manager"] and launch_mode=="serial":
        dask.config.set(scheduler="single-threaded")
    else:
        # If the host is not local, print the job script to be launched in the cluster.
        client, cluster = config_dask.dask_scheduler(jobqueue_config, optimal_n_workers, logdir)
        current_scheduler = dask.config.get('scheduler')
        
    
    logger.info(f"The scheduler after in the launch is {dask.config.get('scheduler')} ")   
    
    if host != "local":
        logger.critical(
            f"The cluster job script for this command is:\n"
            f"{cluster.job_script()}"
        )
        client.close()
        cluster.close()        
        logger.info(f"Client is {client} \n Cluster is {cluster}")
    
    elif host == "local":
        if launch_mode == "parallel":
            logger.critical(
                f"The cluster job script for this command is:\n"
                f"{cluster}"
            )
            client.close()
            cluster.close()
        else:
            logger.critical(
                f"Your launch_mode is {launch_mode}, it will not controlled by dask but go ahead"
            )

    
    # Iterate over the provided subject list
        
    if container in [
        "anatrois",
        "rtppreproc",
        "rtp-pipeline",
        "freesurferator",
        "rtp2-preproc",
        "rtp2-pipeline"
    ]:
        future_dict={}
        future_dict['optimal_n_workers']=optimal_n_workers
        future_dict['container']=container
        future_dict['logdir']=logdir
        lc_configs = []
        subs = []
        sess = []
        analysis_dirs = []
        commands = []
        for row in sub_ses_list.itertuples(index=True, name="Pandas"):
            sub = row.sub
            ses = row.ses

            # Append config, subject, session, and path info in corresponding lists
            lc_configs.append(lc_config)
            subs.append(sub)
            sess.append(ses)
            analysis_dirs.append(ananlysis_dir)

            # This cmd is only for print the command
            command = gen_cmd.dwi_command(
                lc_config,
                sub,
                ses,
                ananlysis_dir
            )
            commands.append(command)
    
            logger.critical(
                f"\nCOMMAND for subject-{sub}, and session-{ses}:\n"
                f"{command}\n\n"
            )
        
        future_dict['lc_configs']=lc_configs
        future_dict['subs']=subs
        future_dict['sess']=sess
        future_dict['analysis_dirs']=analysis_dirs
        future_dict['commands']=commands
    
    elif container in ['l1_glm']:
        
        commands=[]
        future_dict={}
        future_dict['optimal_n_workers']=optimal_n_workers
        future_dict['container']=container
        future_dict['logdir']=logdir
        env_cmd=gen_cmd.py_command()
        for row in sub_ses_list.itertuples(index=True, name="Pandas"):
            sub = row.sub
            ses = row.ses
            
            command= f"{env_cmd}&& python {containerdir}/l1_glm.py --subject {sub} --session {ses} --lc_config {dict_store_cs_configs['lc_yaml_path']} --l1_glm_yaml {dict_store_cs_configs['config_path']} "
            commands.append(command)
            logger.critical(
                f"\nCOMMAND for subject-{sub}, and session-{ses}:\n"
                f"{command}\n\n"
            )
        future_dict['commands']=commands
    return future_dict

def sp_run_cmd(cmd, cmd_id):
    """
    Run a Singularity (Apptainer) command using subprocess and log stdout and stderr.
    Args:
        cmd (str): The Singularity command to run.
        cmd_id (int): Unique identifier for the command to distinguish log entries.
    Returns:
        tuple: stdout and stderr of the command.
    """
    logger.info(f"Executing command {cmd_id}: {cmd}")
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    stdout, stderr = process.communicate()

    if stdout:
        logger.info(f"Command {cmd_id} stdout: {stdout.strip()}")
    if stderr:
        logger.error(f"Command {cmd_id} stderr: {stderr.strip()}")

    return stdout.strip(), stderr.strip()

def launch_dask_futures(
    jobqueue_config,  
    future_dict
    ):
    
    optimal_n_workers=future_dict['optimal_n_workers']
    commands=future_dict['commands']
    launch_mode=jobqueue_config['launch_mode']
    logdir=future_dict['logdir']
    if "local" in jobqueue_config["manager"] and launch_mode=="serial" :
        dask.config.set(scheduler="single-threaded")
        tasks=[dask.delayed(sp_run_cmd)(cmd,i) for i, cmd in enumerate(commands)]
        results=dask.compute(*tasks)
        # Print the results
        print("Results:", results)
        for stdout, stderr in results:
            if stdout:
                logger.info(f"Container stdout: {stdout}")
            if stderr:
                logger.error(f"Container stderr: {stderr}")                
        logger.critical(f"launchcontainer is running in local serial mode")
    else:
        # If the host is not local, print the job script to be launched in the cluster.
        client, cluster = config_dask.dask_scheduler(jobqueue_config, optimal_n_workers, logdir)
        logger.info(
            "---this is the cluster and client\n" + f"{client} \n cluster: {cluster} \n"
        )

        # Compose the command to run in the cluster
        futures = client.map(sp_run_cmd,commands, range(len(commands)))
        logger.info("Dask dashboard is available at:", cluster.dashboard_link)

        # Wait for all jobs to complete
        results = client.gather(futures)
        # Print job results
        for result in results:
            print(result)
        for stdout, stderr in results:
            if stdout:
                logger.info(f"Container stdout: {stdout}")
            if stderr:
                logger.error(f"Container stderr: {stderr}")  
        client.close()
        cluster.close()

        logger.critical("\n" + "launchcontainer finished, all the jobs are done")


# %% main()
def main():
    parser_namespace,parse_dict = do.get_parser()
    download_configs=parser_namespace.download_configs
    gen_subseslist=parser_namespace.gen_subseslist
    print(parse_dict)
    print(gen_subseslist)
    # generate template subseslist under the working directory
    if gen_subseslist:
        if parser_namespace.sub is None or parser_namespace.ses is None:
            raise ValueError("gen_subseslist requires -sub and -ses to be provided")
        else:
            sub_list = parser_namespace.sub
            ses_list = parser_namespace.ses
            do.generate_subseslist(sub_list,ses_list)
            print("\n######Your template sub_ses_list.txt has been created under the CWD!######")
        return
    # Check if download_configs argument is provided
    if download_configs:
        # Ensure the directory exists
        if not os.path.exists(download_configs):
            os.makedirs(download_configs)
        
        # Use the mocked version function for testing
        launchcontainers_version = do.get_mocked_launchcontainers_version()
        
        if launchcontainers_version is None:
            raise ValueError("Unable to determine launchcontainers version.")
        do.download_configs(launchcontainers_version, download_configs)
        print("\n######Your example configs has been copied to your indicated directory created under the CWD!######")
        return
    # main function with PREPARE and RUN mode
    if (not gen_subseslist) or (not download_configs): 
        print("**********Executing main functionality with arguments*********")
        # read ymal and setup the launchcontainer program
        lc_config_path = parser_namespace.lc_config
        lc_config = do.read_yaml(lc_config_path)
        run_lc = parser_namespace.run_lc
        verbose = parser_namespace.verbose
        debug = parser_namespace.debug
        # Get general information from the config.yaml file
        basedir=lc_config["general"]["basedir"]
        bidsdir_name=lc_config["general"]["bidsdir_name"]
        container=lc_config["general"]["container"]
        analysis_name=lc_config["general"]["analysis_name"]
        host=lc_config["general"]["host"]
        print_command_only=lc_config["general"]["print_command_only"]
        log_dir=lc_config["general"]["log_dir"]
        log_filename=lc_config["general"]["log_filename"]
        
        version = lc_config["container_specific"][container]["version"] 
        jobqueue_config = lc_config["host_options"][host]
        # get stuff from subseslist for future jobs scheduling
        sub_ses_list_path = parser_namespace.sub_ses_list
        sub_ses_list,num_of_true_run = do.read_df(sub_ses_list_path)

        if log_dir=="analysis_dir":
            log_dir=op.join(basedir,bidsdir_name,'derivatives',f'{container}_{version}',f"analysis-{analysis_name}")

        do.setup_logger(print_command_only,verbose, debug, log_dir, log_filename)
        
        # logger the settings
        if host == "local":
            launch_mode = lc_config["host_options"]["local"]["launch_mode"]
            valid_options = ["serial", "parallel","dask_worker"]
            if launch_mode in valid_options:
                host_str = (
                    f"{host}, and commands will be launched in {launch_mode} mode "
                    f"Serial is safe but it will take longer. "
                    f"If you launch in parallel be aware that some of the "
                    f"processes might be killed if the limit (usually memory) "
                    f"of the machine is reached. "
                )
            else:
                do.die(
                    f"local:launch_mode {launch_mode} was passed, valid options are {valid_options}"
                )

        logger.critical(
            "\n"
            + "#####################################################\n"
            + f"Successfully read the config file {lc_config_path} \n"
            + f"SubsesList is read, there are {num_of_true_run} jobs needed to be launched"
            + f'Basedir is: {lc_config["general"]["basedir"]} \n'
            + f'Container is: {container}_{lc_config["container_specific"][container]["version"]} \n'
            + f"Host is: {host_str} \n"
            + f'analysis folder is: {lc_config["general"]["analysis_name"]} \n'
            + f"##################################################### \n"
        )
       
        
        # Prepare file and launch containers
        # First of all prepare the analysis folder: it create you the analysis folder automatically so that you are not messing up with different analysis
        ananlysis_dir, dict_store_cs_configs = (
            prepare.prepare_analysis_folder(parser_namespace, lc_config)
        )
        container_configs_under_analysis_folder=dict_store_cs_configs['config_path']
        
        logger.info("Reading the BIDS layout...")
        layout = BIDSLayout(os.path.join(basedir, bidsdir_name))
        logger.info("finished reading the BIDS layout.")
        
        # Prepare mode
        # if DWI Pipeline (preproc, pipeline)
        if container in [
            "anatrois",
            "rtppreproc",
            "rtp-pipeline",
            "freesurferator",
            "rtp2-preproc",
            "rtp2-pipeline"
        ]:  
            logger.debug(f"{container} is in the list")
            sub_ses_list= sub_ses_list[(sub_ses_list['dwi'] == "True") & (sub_ses_list['RUN'] == "True")]
            prepare.prepare_dwi_input(
                parser_namespace, ananlysis_dir, lc_config, sub_ses_list, layout, dict_store_cs_configs
            )
            
            future_dict= prepare_dask_futures(
            ananlysis_dir,
            lc_config,
            sub_ses_list,
            dict_store_cs_configs
        )
        elif container in ["l1_glm"]:   
            sub_ses_list= sub_ses_list[(sub_ses_list['func'] == "True") & (sub_ses_list['RUN'] == "True")]
            prepare.prepare_fmri_input(
                parser_namespace, ananlysis_dir, lc_config, sub_ses_list, dict_store_cs_configs
            )
            future_dict= prepare_dask_futures(
            ananlysis_dir,
            lc_config,
            sub_ses_list,
            dict_store_cs_configs
        )
        elif container in ["fmriprep"]:
            sub_ses_list= sub_ses_list["only have the subs, because it will not get the sessions"]
        else:
            logger.error(f"{container} is not in the list")
            raise KeyError("The container name you input is not supported, can't do prepare or launch jobs")


        if run_lc:
            launch_dask_futures(jobqueue_config,future_dict)
if __name__ == "__main__":
    main()
