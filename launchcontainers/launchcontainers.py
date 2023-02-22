import argparse
from logging import DEBUG
import os
import shutil as sh
import glob
import subprocess as sp
from xxlimited import Str
import numpy as np
import pandas as pd
import json
import sys
#from launchcontainers import __version__
import yaml
from yaml.loader import SafeLoader
import pip
package='nibabel'
# !{sys.executable} -m pip install nibabel  # inside jupyter console
def import_or_install(package):
    try:
        __import__(package)
    except ImportError:
        pip.main(['install', package])
import_or_install(package)
import nibabel as nib
import createsymlinks as csl
import glob
"""
TODO: 
    4./ Add the check in launchcontainers.py, that only in some cases we wiill need to use createSymLinks, and for the anatrois, rtppreproc and rtp-pipeline, we will need to do it
    5./ Edit createSymLinks again and make one function per every container
        createSymLinks_anatrois.py
        createSymLinks_rtppreproc.py
        createSymLinks_rtp-pipeline.py
"""
sys.path.insert(0,'/Users/tiger/Documents/GitHub/launchcontainers/launchcontainers')
#%% parser
def _get_parser():
    """
    Input: 
    Parse command line inputs
    
    Returns:
    a dict stores information about the configFile and subSesList
    
    Notes:
    # Argument parser follow template provided by RalphyZ.
    # https://stackoverflow.com/a/43456577
    """
    parser = argparse.ArgumentParser(description='''createSymLinks.py 'pathTo/config_launchcontainers.yaml' ''')
    parser.add_argument('--configFile', 
                        type=str, 
                        default="/Users/tiger/TESTDATA/PROJ01/nifti/config_launchcontainer_copy.yaml",
                        help='path to the config file')
    parser.add_argument('--subSesList', 
                        type=str,
                        default="/Users/tiger/TESTDATA/PROJ01/nifti/subSesList.txt",
                        help='path to the config file')
    parse_result  = vars(parser.parse_args())

    return parse_result

#%% function to read config file, yaml
def _read_config(path_to_config_file):
    ''' 
    Input:
    the path to the config file 
    
    Returns
    a dictionary that contains all the config info
    
    '''
    print(f'Read the config file {path_to_config_file} ')

    with open(path_to_config_file, 'r') as v:
        config = yaml.load(v, Loader=SafeLoader)
    
    container = config["config"]["container"]

    print(f'\nBasedir is: {config["config"]["basedir"]}')
    print(f'\nContainer is: {container}_{config["container_options"][container]["version"]}')
    print(f'\nAnalysis is: analysis-{config["config"]["analysis"]}')

    return config

#%% function to read subSesList. txt
def _read_subSesList(path_to_subSesList_file):
    ''' 
    Input:
    path to the subject and session list txt file 
    
    Returns
    a dataframe
    
    '''
    subSesList  = pd.read_csv(path_to_subSesList_file, sep=",", header=0)

    return subSesList

#%% Launchcontainer
def prepare_input_file(config_dict, df_subSes):
    """
    
    Parameters
    ----------
    config_dict : TYPE
        DESCRIPTION.
    df_subSes : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """
    for row in df_subSes.itertuples(index= True, name = "Pandas"):
        sub = row.sub
        ses = row.ses
        RUN = row.RUN
        dwi = row.dwi
        func = row.func
        container = config_dict['config']['container']
        version = config_dict['container_options'][container]['version']
        print(f"{sub}_{ses}_RUN-{RUN}_{container}_{version}")
        
        if not RUN: 
            continue
        
        if "rtppreproc" in container:
            csl.rtppreproc(config_dict, sub, ses)
        elif "rtp-pipeline" in container:
            csl.rtppipeline(config_dict, sub, ses)
        elif "anatrois" in container:
            csl.anatrois(config_dict, sub, ses)
        #future container
        else:
            print(f"{container} is not created, check for typos or if it is a new container create it in launchcontainers.py")
   
    return 

def launchcontainers(
    subSes_df,
    config_dict,
    tmp_path,
    log_path,
):
    """
    This function launches containers generically in different Docker/Singularity HPCs
    This function is going to assume that all files are where they need to be. 

    Parameters
    ----------
    list_path : str
        The path to the subject list (.txt).
    config_path : str
        The path to the configuration file.
    """


    # If tmpdir and logdir do not exist, create them
    if not os.path.isdir(tmp_path): os.mkdir(tmp_path)
    if not os.path.isdir(log_path): os.mkdir(log_path)

    # Get the unique list of subjects and sessions
    
    #*** OLD delete
    #subseslist=os.path.join(basedir,"Nifti","subSesList.txt")
    #*** end OLD delete
    
    
    for row in subSes_df.itertuples(index=True, name='Pandas'):
        sub  = row.sub
        ses  = row.ses
        RUN  = row.RUN
        dwi  = row.dwi
        func = row.func
        if RUN and dwi:
            cmdstr = (f"{codedir}/qsub_generic.sh " +
                    f"-t {tool} " +
                    f"-s {sub} " +
                    f"-e {ses} " +
                    f"-a {analysis} " +
                    f"-b {basedir} " +
                    f"-o {codedir} " +
                    f"-m {mem} " +
                    f"-q {que} " +
                    f"-c {core} " +
                    f"-p {tmpdir} " +
                    f"-g {logdir} " +
                    f"-i {sin_ver} " +
                    f"-n {container} " +
                    f"-u {qsub} " +
                    f"-h {host} " +
                    f"-d {manager} " +
                    f"-f {system} " +
                    f"-j {maxwall} ")
            
            print(cmdstr)
            sp.call(cmdstr, shell=True)
#%% main()
def main():
    """launch_container entry point"""
    inputs       = _get_parser()
    config_dict  = _read_config(inputs['configFile'])
    subSes_df    = _read_subSesList(inputs['subSesList'])
    
    prepare_input_file(config_dict, subSes_df)
    
    # launchcontainers('kk', command_str=command_str)
    
    # backup_configinfo()



#%% Run main()
'''
One of the TODO:
    make a function, when this file was run, create a copy of the original yaml file
    then rename it , add the date and time in the end
    stored the new yaml file under the output folder
'''

# #%%
if __name__ == "__main__":
    main()
    