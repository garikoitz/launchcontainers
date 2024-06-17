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
import os.path as op
from os import rename
from os import path, symlink, unlink
import json
from glob import glob

import numpy as np 
from scipy.io import loadmat

from . import utils as do
from . import prepare_dwi as dwipre

logger = logging.getLogger("Launchcontainers")


#%% copy configs or create new analysis
def prepare_analysis_folder(parser_namespace, lc_config):
    '''
    this function is the very very first step of everything, it is IMPORTANT, 
    it will provide a check if your desired analysis has been running before
    and it will help you keep track of your input parameters so that you know what you are doing in your analysis    

    the option force will not be useful at the analysis_folder level, if you insist to do so, you need to delete the old analysis folder by hand
    
    after determine the analysis folder, this function will copy your input configs to the analysis folder, and it will read only from there
    '''
    # read parameters from lc_config
    basedir = lc_config['general']['basedir']
    container = lc_config['general']['container']
    force = lc_config["general"]["force"]
    analysis_name= lc_config['general']['analysis_name']
    run_lc = parser_namespace.run_lc
    force= force or run_lc    
    version = lc_config["container_specific"][container]["version"]    
    bidsdir_name = lc_config['general']['bidsdir_name']  
    container_folder = op.join(basedir, bidsdir_name,'derivatives',f'{container}_{version}')
    if not op.isdir(container_folder):
        os.makedirs(container_folder)
    
    analysis_dir = op.join(
        container_folder,
        f"analysis-{analysis_name}",
                )
    
    # define the potential exist config files
    lc_config_under_analysis_folder = op.join(analysis_dir, "lc_config.yaml")
    subSeslist_under_analysis_folder = op.join(analysis_dir, "subSesList.txt")
    
    cc_number={
        'anatrois':1 ,
        'freesurferator':1 ,
        'rtppreproc':1 ,
        'rtp2-preproc': 1 ,
        'rtp-pipeline': 2 , 
        'rtp2-pipeline': 2
    }

    if container  not in ['rtp-pipeline', 'rtp2-pipeline','fmriprep']:    
        container_configs_under_analysis_folder = [op.join(analysis_dir, "config.json")] 
    elif container in ['rtp-pipeline', 'rtp2-pipeline']:
        container_configs_under_analysis_folder = [op.join(analysis_dir, "config.json"), op.join(analysis_dir, "tractparams.csv")]
    
    if not op.isdir(analysis_dir):
        os.makedirs(analysis_dir)
    # copy the config under the analysis folder
    do.copy_file(parser_namespace.lc_config, lc_config_under_analysis_folder, force) 
    do.copy_file(parser_namespace.sub_ses_list,subSeslist_under_analysis_folder,force)
    for orig_config_json, copy_config_json in zip(parser_namespace.container_specific_config, container_configs_under_analysis_folder):
        do.copy_file(orig_config_json, copy_config_json, force)    
    logger.debug(f'\n The analysis folder is {analysis_dir}, all the configs has been copied') 

    copies = [lc_config_under_analysis_folder, subSeslist_under_analysis_folder] + container_configs_under_analysis_folder

    all_copies_present= all(op.isfile(copy_path) for copy_path in copies)

    if all_copies_present:
        pass
    else:
        logger.error(f'\n did NOT detect back up configs in the analysis folder, Please check then continue the run mode')

    return analysis_dir, container_configs_under_analysis_folder

# %% prepare_input_files
def prepare_dwi_input(parser_namespace, analysis_dir, lc_config, df_subSes, layout, container_configs_under_analysis_folder):
    """
    This is the major function for doing the preparation, it is doing the work 
    1. write the config.json (analysis level)
    2. create symlink for input files (subject level)
    
    Parameters
    ----------
    lc_config : TYPE
        DESCRIPTION.
    df_subSes : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """
    logger.info("\n"+
                "#####################################################\n"
                +"Preparing for DWI pipeline RTP2\n")
    
    # first check, if the container specific config is passed, if not, prepare will stop
    if len(parser_namespace.container_specific_config)==0:
                logger.critical("\n"
                              +"Input file error: the container specific config is not provided")
                raise FileNotFoundError("Didn't input container_specific_config, please indicate it in your command line flag -cc")
    
    container = lc_config["general"]["container"]
    force = lc_config["general"]["force"]   
    run_lc = parser_namespace.run_lc    
    force= force or run_lc    
    version = lc_config["container_specific"][container]["version"]
    
    logger.info("\n"+
                "#####################################################\n"
                +f"Prepare 1, write config.json RTP2-{container}\n")
    
    if prepare_dwi_config_json(parser_namespace,lc_config,force):
        logger.info("\n"+
                "#####################################################\n"
                +f"Prepare 1, finished\n")
    else:
        logger.critical("\n"+
                "#####################################################\n"
                +f"Prepare json not finished. Please check\n")
        raise Exception("Sorry the Json file seems not being written correctly, it may cause container dysfunction")

    logger.info("\n"+
                "#####################################################\n"
                +f"Prepare 2, create the symlinks of all the input files RTP2-{container}\n")
    
    for row in df_subSes.itertuples(index=True, name="Pandas"):
        sub = row.sub
        ses = row.ses
        RUN = row.RUN
        dwi = row.dwi
        
        logger.info(f'dwi is {dwi}')
        logger.info("\n"
                    +"The current run is: \n"
                    +f"{sub}_{ses}_{container}_{version}\n")
        

        if RUN == "True" and dwi == "True":
                        
            tmpdir = op.join(
                analysis_dir,
                "sub-" + sub,
                "ses-" + ses,
                "output", "tmp"
            )
            logdir = op.join(
                analysis_dir,
                "sub-" + sub,
                "ses-" + ses,
                "output", "log"
            )

            if not op.isdir(tmpdir):
                os.makedirs(tmpdir)
            logger.info(f"\n the tmp dir is created at {tmpdir}, and it is {op.isdir(tmpdir)} that this file exists")
            if not op.isdir(logdir):
                os.makedirs(logdir)
            
            do.copy_file(parser_namespace.lc_config, op.join(logdir,'lc_config.yaml'), force) 
               
  

            if container in ["rtppreproc" ,"rtp2-preproc"]:
                do.copy_file(container_configs_under_analysis_folder[0], op.join(logdir,'config.json'), force)
                dwipre.rtppreproc(parser_namespace, analysis_dir, lc_config, sub, ses, layout)
            
            elif container in ["rtp-pipeline", "rtp2-pipeline"]:
                
                if not len(parser_namespace.container_specific_config) == 2:
                    logger.error("\n"
                              +f"Input file error: the RTP-PIPELINE config is not provided completely")
                    raise FileNotFoundError('The RTP-PIPELINE needs the config.json and tratparams.csv as container specific configs')
                
                do.copy_file(container_configs_under_analysis_folder[0],op.join(logdir, "config.json"), force) 
                do.copy_file(container_configs_under_analysis_folder[-1],op.join(logdir, "tractparams.csv"), force) 
                
                dwipre.rtppipeline(parser_namespace, analysis_dir,lc_config, sub, ses, layout)
            
            elif container in ["anatrois","freesurferator"]:
                do.copy_file(container_configs_under_analysis_folder[0], op.join(logdir,'config.json'), force)
                dwipre.anatrois(parser_namespace, analysis_dir,lc_config,sub, ses, layout)
            
            else:
                logger.error("\n"+
                             f"***An error occurred"
                             +f"{container} is not created, check for typos or contact admin for singularity images\n"
                )
        else:
            continue
    logger.info("\n"+
                "#####################################################\n")
    return  

def prepare_dwi_config_json(parser_namespace,lc_config,force):
    '''
    This function is used to automatically read config.yaml and get the input file info and put them in the config.json
    
    '''
    
    def write_json(config_json_extra, json_file_input_path, json_file_output_path, force):
        config_json_instance = json.load(open(json_file_input_path))
        if not "input" in config_json_instance:
            config_json_instance["inputs"] = config_json_extra
        else:
            logger.warn(f"{json_file_input_path} json file already has field input, we will overwrite it if you set force to true")
            if force:
               config_json_instance["inputs"] = config_json_extra
            else:
                pass         
        with open(json_file_output_path , "w") as outfile:
            json.dump(config_json_instance, outfile, indent = 4)
        
        return True
    
    def get_config_dict(container,lc_config,rtp2_json_keys,rtp2_json_val):
        config_info_dict = {}
        yaml_info=lc_config["container_specific"][container]
        
        rtp2_json_dict= {key: value for key, value in zip(rtp2_json_keys, rtp2_json_val)}

        
        if container == "freesurferator":
            config_json_extra={'anat': 
                        {'location': {
                            'path': '/flywheel/v0/input/anat/T1.nii.gz', 
                            'name': 'T1.nii.gz',
                        },
                        'base': 'file'}
                        }
            for key in rtp2_json_dict.keys():
                if key in yaml_info.keys() and yaml_info[key]:
                    config_json_extra[key] = {
                            'location': {
                                'path': op.join('/flywheel/v0/input', rtp2_json_dict[key]), 
                                'name': op.basename(rtp2_json_dict[key])
                            },
                            'base': 'file'
                        }
                if 'anat' in config_json_extra.keys() and 'pre_fs' in config_json_extra.keys():
                    config_json_extra.pop('anat')

        else:
            config_json_extra={}
            for key in rtp2_json_dict.keys():
                config_json_extra[key] = {
                        'location': {
                            'path': op.join('/flywheel/v0/input', rtp2_json_dict[key]), 
                            'name': op.basename(rtp2_json_dict[key])
                        },
                        'base': 'file'
                    }
               
        return config_json_extra

    container = lc_config["general"]["container"]   
    
    
    if container == "freesurferator":
        fs_json_keys=['pre_fs','control_points','annotfile', 'mniroizip']
        fs_json_val=['pre_fs/existingFS.zip','control_points/control.dat','mniroizip/mniroizip.zip','annotfile/annotfile.zip']
        config_json_extra=get_config_dict(container,lc_config,fs_json_keys,fs_json_val)
        json_file_input_path=parser_namespace.container_specific_config[0]
        json_file_output_path=parser_namespace.container_specific_config[0]
        if write_json(config_json_extra, json_file_input_path, json_file_output_path,force):
            logger.info(f"Successfully write json for {container}")
        
        # TODO:
        # "t1w_anatomical_2": gear_context.get_input_path("t1w_anatomical_2"),
        # "t1w_anatomical_3": gear_context.get_input_path("t1w_anatomical_3"),
        # "t1w_anatomical_4": gear_context.get_input_path("t1w_anatomical_4"),
        # "t1w_anatomical_5": gear_context.get_input_path("t1w_anatomical_5"),
        # "t2w_anatomical": gear_context.get_input_path("t2w_anatomical"),
         
    if container in "rtp2-preproc":
        preproc_json_keys=['ANAT','BVAL','BVEC', 'DIFF','FSMASK']
        preproc_json_val=['ANAT/T1.nii.gz','BVAL/dwiF.bval','BVEC/dwiF.bvec','DIFF/dwiF.nii.gz','FSMASK/brainmask.nii.gz']
        config_json_extra=get_config_dict(container,lc_config,preproc_json_keys,preproc_json_val)
        json_file_input_path=parser_namespace.container_specific_config[0]
        json_file_output_path=parser_namespace.container_specific_config[0]
        if write_json(config_json_extra, json_file_input_path, json_file_output_path,force):
            logger.info(f"Successfully write json for {container}")

    if container in "rtp2-pipeline":
        pipeline_json_keys=['anatomical','bval','bvec', 'dwi','fs','tractparams']
        pipeline_json_val=['anatomical/T1.nii.gz','bval/dwi.bval','bvec/dwi.bvec','dwi/dwi.nii.gz','fs/fs.zip','tractparams/tractparams.csv']
        config_json_extra=get_config_dict(container,lc_config,pipeline_json_keys,pipeline_json_val)
        json_file_input_path=parser_namespace.container_specific_config[0]
        json_file_output_path=parser_namespace.container_specific_config[0]
        if write_json(config_json_extra, json_file_input_path, json_file_output_path,force):
            logger.info(f"Successfully write json for {container}")
        # "fsmask": gear_context.get_input_path("fsmask"),
    
    return True   

