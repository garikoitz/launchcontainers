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
import json
import zipfile

# for package mode, the import needs to import launchcontainer module
from launchcontainers.prepare_inputs import utils as do
from launchcontainers.prepare_inputs import prepare_dwi as dwipre

# for testing mode using repo
# from prepare_inputs import utils as do
# from prepare_inputs import prepare_dwi as dwipre

logger = logging.getLogger("Launchcontainers")

def prepare_analysis_folder(parser_namespace, lc_config):
    '''
    Description: create analysis folder based on your container and your analysis.
    
    In the meantime, it will copy your input config files to the analysis folder. 

    In the end, it will check if everything are in place and ready for the next level preparation 
    which is at the subject and session level

    After this step, the following preparing method will based on the config files under the analysis folder instread of your input
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
    if not op.isdir(analysis_dir):
        os.makedirs(analysis_dir)
    
    # create log dir for dask
    host = lc_config["general"]["host"]
    jobqueue_config = lc_config["host_options"][host]
    daskworer_logdir = os.path.join(analysis_dir, "daskworker_log")
    if jobqueue_config["manager"] in ["sge","slurm"] and  not os.path.exists(daskworer_logdir):
        os.makedirs(daskworer_logdir)
    if jobqueue_config["manager"] in ["local"]: 
        if (jobqueue_config["launch_mode"]=='dask_worker'):
             os.makedirs(daskworer_logdir)
    ############################################################################################
    ############################Copy the configs################################################
    ############################################################################################
    # define the potential exist config files
    lc_config_under_analysis_folder = op.join(analysis_dir, "lc_config.yaml")
    subSeslist_under_analysis_folder = op.join(analysis_dir, "subSesList.txt")
    # the name of container_specific configs is consitant with your input name, it is not necessary a .json file
    container_configs_under_analysis_folder = op.join(analysis_dir,os.path.basename(parser_namespace.container_specific_config))
    
    # then we have optional configs we need to add to the list
    # copy the config under the analysis folder
    do.copy_file(parser_namespace.lc_config, lc_config_under_analysis_folder, force) 
    do.copy_file(parser_namespace.sub_ses_list,subSeslist_under_analysis_folder,force)
    do.copy_file(parser_namespace.container_specific_config, container_configs_under_analysis_folder, force)    
    
    logger.debug(f'\n The analysis folder is {analysis_dir}, all the configs has been copied') 
    
    dict_store_cs_configs={}
    dict_store_cs_configs['config_path']=container_configs_under_analysis_folder
    
    def process_optional_input(container,file_path, analysis_dir, option=None):
        if os.path.isfile(file_path):
            logger.info("\n"
                +f" You have choossen to pass  {file_path} to {container}, it will be first copy to {analysis_dir}")
        else:
            logger.error("\n"
                        f"{file_path} does not exist")        
        
        file_name = os.path.basename(file_path)
        file_ext = os.path.splitext(file_name)[1]
        
        if container in ['anatrois','freesurferator']:
            if file_ext in ['.nii', '.gz','.zip']:
                do.copy_file(file_path, os.path.join(analysis_dir, file_name), force)          
            else:
                raise ValueError("Unsupported file type.")
        if container in ['rtp2-preproc','rtppreproc']:
            if file_ext in ['.nii', '.gz']:
                do.copy_file(file_path, os.path.join(analysis_dir, file_name), force)                          
            else:
                raise ValueError("Unsupported file type.")
                    
        if container in ['rtp2-pipeline','rtp-pipeline']:
            if option == "tractparams":
                if file_ext in ['.csv']:    
                    do.copy_file(file_path, os.path.join(analysis_dir, file_name), force)
                else:
                    raise ValueError("Unsupported file type.")                    
            if option == "fsmask":
                if file_ext in ['.nii', '.gz']:
                    do.copy_file(file_path, os.path.join(analysis_dir, file_name), force)            
                else:
                    raise ValueError("Unsupported file type.")                
            if option == "qmap_zip":
                if file_ext == '.zip':
                    do.copy_file(file_path, os.path.join(analysis_dir, file_name), force)          
                else:
                    raise ValueError("Unsupported file type.")
        return file_name
    
    # copy annotfile or mnizip file to analysis folder
    if container in ['anatrois']:
        dict_store_cs_configs[container]={}
        pre_fs= lc_config["container_specific"][container]["pre_fs"]
        annotfile = lc_config["container_specific"][container]["annotfile"]
        mniroizip = lc_config["container_specific"][container]["mniroizip"]
        if pre_fs:
            file_name="existingFS.zip"
            dict_store_cs_configs[container]['pre_fs']=f"pre_fs/{file_name}"         
        if annotfile:
            file_name=process_optional_input(container,annotfile,analysis_dir)
            dict_store_cs_configs[container]['annotfile']=f"annotfile/{file_name}"
        if mniroizip:
            file_name=process_optional_input(container,mniroizip,analysis_dir)
            dict_store_cs_configs[container]['mniroizip']=f"mniroizip/{file_name}"
        # copy annotfile or mnizip file to analysis folder
    if container in ['freesurferator']:
        dict_store_cs_configs[container]={}
        pre_fs= lc_config["container_specific"][container]["pre_fs"]
        control_points= lc_config["container_specific"][container]["control_points"]
        annotfile = lc_config["container_specific"][container]["annotfile"]
        mniroizip = lc_config["container_specific"][container]["mniroizip"]
        if pre_fs:
            file_name="existingFS.zip"
            dict_store_cs_configs[container]['pre_fs']=f"pre_fs/{file_name}"
        if control_points:
            file_name="control.dat"
            dict_store_cs_configs[container]['control_points']=f"control_points/{file_name}"            
        if annotfile:
            file_name=process_optional_input(container,annotfile,analysis_dir)
            dict_store_cs_configs[container]['annotfile']=f"annotfile/{file_name}"
        if mniroizip:
            file_name=process_optional_input(container,mniroizip,analysis_dir)
            dict_store_cs_configs[container]['mniroizip']=f"mniroizip/{file_name}"
    # copy qmap.nii of qmap.nii.gz to analysis folder
    if container in ['rtppreproc']:
        preproc_json_keys=['ANAT','BVAL','BVEC', 'DIFF','FSMASK']
        preproc_json_val=['ANAT/T1.nii.gz','BVAL/dwiF.bval','BVEC/dwiF.bvec','DIFF/dwiF.nii.gz','FSMASK/brain.nii.gz']
        dict_store_cs_configs[container]= {key: value for key, value in zip(preproc_json_keys, preproc_json_val)}
        rpe=lc_config["container_specific"][container]["rpe"]
        if rpe:
            dict_store_cs_configs[container]['RBVC']= 'RBVC/dwiR.bvec'
            dict_store_cs_configs[container]['RBVL']= 'RBVL/dwiR.bval'
            dict_store_cs_configs[container]['RDIF']= 'RDIF/dwiR.nii.gz'            
    if container in ['rtp2-preproc']:
        preproc_json_keys=['ANAT','BVAL','BVEC', 'DIFF','FSMASK']
        preproc_json_val=['ANAT/T1.nii.gz','BVAL/dwiF.bval','BVEC/dwiF.bvec','DIFF/dwiF.nii.gz','FSMASK/brain.nii.gz']
        dict_store_cs_configs[container]= {key: value for key, value in zip(preproc_json_keys, preproc_json_val)}
        
        rpe=lc_config["container_specific"][container]["rpe"]
        qmap=lc_config["container_specific"][container]["qmap_nifti"]
        if rpe:
            dict_store_cs_configs[container]['RBVC']= 'RBVC/dwiR.bvec'
            dict_store_cs_configs[container]['RBVL']= 'RBVL/dwiR.bval'
            dict_store_cs_configs[container]['RDIF']= 'RDIF/dwiR.nii.gz'            
        if qmap:
            file_name=process_optional_input(container,qmap,analysis_dir)
            dict_store_cs_configs[container]['qmap']=f"qmap/{file_name}"
        
    if container in ['rtp-pipeline']:
        pipeline_json_keys=['anatomical','bval','bvec', 'dwi','fs']
        pipeline_json_val=['anatomical/T1.nii.gz','bval/dwi.bval','bvec/dwi.bvec','dwi/dwi.nii.gz','fs/fs.zip']
        dict_store_cs_configs[container]= {key: value for key, value in zip(pipeline_json_keys, pipeline_json_val)}
        
        tractparams=lc_config["container_specific"][container]["tractparams"]
        if tractparams:
            file_name=process_optional_input(container,tractparams,analysis_dir,"tractparams")    
            dict_store_cs_configs[container]['tractparams']=f"tractparams/{file_name}"
    if container in ['rtp2-pipeline']:
        pipeline_json_keys=['anatomical','bval','bvec', 'dwi','fs']
        pipeline_json_val=['anatomical/T1.nii.gz','bval/dwi.bval','bvec/dwi.bvec','dwi/dwi.nii.gz','fs/fs.zip']
        dict_store_cs_configs[container]= {key: value for key, value in zip(pipeline_json_keys, pipeline_json_val)}        
        tractparams=lc_config["container_specific"][container]["tractparams"]
        fsmask=lc_config["container_specific"][container]["fsmask"]
        qmap_zip=lc_config["container_specific"][container]["qmap_zip"]
        if tractparams:
            file_name=process_optional_input(container,tractparams,analysis_dir,"tractparams")
            dict_store_cs_configs[container]['tractparams']=f"tractparams/{file_name}"
        if fsmask:
            file_name=process_optional_input(container,fsmask,analysis_dir,"fsmask")
            dict_store_cs_configs[container]['fsmask']=f"fsmask/{file_name}"
        if qmap_zip:
            file_name=process_optional_input(container,qmap_zip,analysis_dir,"qmap_zip")           
            dict_store_cs_configs[container]['qmap_zip']=f"qmap_zip/{file_name}"       


    ############################################################################################
    ############################Do the checks###################################################
    ############################################################################################

    copies = [lc_config_under_analysis_folder, subSeslist_under_analysis_folder, container_configs_under_analysis_folder]

    all_copies_present= all(op.isfile(copy_path) for copy_path in copies)

    if all_copies_present:
        pass
    else:
        logger.error(f'\n did NOT detect back up configs in the analysis folder, Please check then continue the run mode')

    return analysis_dir, dict_store_cs_configs

def prepare_dwi_config_json(dict_store_cs_configs,lc_config,force):
    '''
    This function is used to automatically read config.yaml and get the input file info and put them in the config.json
    
    '''
    
    def write_json(config_json_extra, json_under_analysis_dir, force):
        config_json_instance = json.load(open(json_under_analysis_dir))
        if not "input" in config_json_instance:
            config_json_instance["inputs"] = config_json_extra
        else:
            logger.warn(f"{json_under_analysis_dir} json file already has field input, we will overwrite it if you set force to true")
            if force:
               config_json_instance["inputs"] = config_json_extra
            else:
                pass         
        with open(json_under_analysis_dir , "w") as outfile:
            json.dump(config_json_instance, outfile, indent = 4)
        
        return True
    
    def get_config_dict(container,lc_config,dict_store_cs_configs):
        
        yaml_info=lc_config["container_specific"][container]
        
        rtp2_json_dict= dict_store_cs_configs[container]

        
        if container in ["freesurferator", "anatrois"]:
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
            if 'anat' in  config_json_extra.keys() and 'pre_fs' in config_json_extra.keys():
                del config_json_extra['anat']
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
    config_json_extra=get_config_dict(container,lc_config,dict_store_cs_configs)
    
    json_under_analysis_dir=dict_store_cs_configs['config_path']

    if write_json(config_json_extra, json_under_analysis_dir,force):
        logger.info(f"Successfully write json for {container}")     
    return True   


def prepare_dwi_input(parser_namespace, analysis_dir, lc_config, df_subSes, layout, dict_store_cs_configs):
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
    
    
    container = lc_config["general"]["container"]
    force = lc_config["general"]["force"]   
    run_lc = parser_namespace.run_lc    
    force= force or run_lc    
    version = lc_config["container_specific"][container]["version"]
    
    logger.info("\n"+
                "#####################################################\n"
                +f"Prepare 1, write config.json RTP2-{container}\n")
    
    if prepare_dwi_config_json(dict_store_cs_configs,lc_config,force):
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
            container_logdir = op.join(
                analysis_dir,
                "sub-" + sub,
                "ses-" + ses,
                "output", "log"
            )

            if not op.isdir(tmpdir):
                os.makedirs(tmpdir)
            if not op.isdir(container_logdir):
                os.makedirs(container_logdir)
            
            do.copy_file(parser_namespace.lc_config, op.join(container_logdir,'lc_config.yaml'), force) 
            config_file_path=dict_store_cs_configs['config_path']
            do.copy_file(config_file_path, op.join(container_logdir,'config.json'), force)   


            if container in ["rtppreproc" ,"rtp2-preproc"]:
                dwipre.rtppreproc(dict_store_cs_configs, analysis_dir, lc_config, sub, ses, layout,run_lc)
            elif container in ["rtp-pipeline", "rtp2-pipeline"]:
                dwipre.rtppipeline(dict_store_cs_configs, analysis_dir,lc_config, sub, ses, layout,run_lc)
            elif container in ["anatrois","freesurferator"]:
                dwipre.anatrois(dict_store_cs_configs, analysis_dir,lc_config,sub, ses, layout,run_lc)
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

