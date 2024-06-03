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
import os.path as path
from os import rename
from os import path, symlink, unlink
import sys
import json
from glob import glob

import numpy as np 
from scipy.io import loadmat

from . import utils as do
from . import prepare_dwi as dwipre

logger=logging.getLogger("GENERAL")


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
    # get the analysis folder information
    
    bidsdir_name = lc_config['general']['bidsdir_name']  

    container_folder = os.path.join(basedir, bidsdir_name,'derivatives',f'{container}_{version}')
    if not os.path.isdir(container_folder):
        os.makedirs(container_folder)
    
    

    Dir_analysis = os.path.join(
        container_folder, ##########before , there is _{version}
        f"analysis-{analysis_name}",
                )
    
        
    # define the potential exist config files
    path_to_analysis_lc_config = os.path.join(Dir_analysis, "lc_config.yaml")
    path_to_analysis_sub_ses_list = os.path.join(Dir_analysis, "subSesList.txt")
    
    if container  not in ['rtp-pipeline', 'fmriprep']:    
        path_to_analysis_container_specific_config = [os.path.join(Dir_analysis, "config.json")] 
    if container == 'rtp-pipeline':
        path_to_analysis_container_specific_config = [os.path.join(Dir_analysis, "config.json"), os.path.join(Dir_analysis, "tractparams.csv")]
    if container == 'fmriprep':
        path_to_analysis_container_specific_config=[]
    #TODO: heudiconv, nordic, presurfer
    


    if not run_lc:
        logger.warning(f'\nthis is PREPARE MODE, starts to  create analysis folder and copy the configs')
        if not os.path.isdir(Dir_analysis):
            os.makedirs(Dir_analysis)
        
        # backup the config info
        
        do.copy_file(parser_namespace.lc_config, path_to_analysis_lc_config, force) 
        do.copy_file(parser_namespace.sub_ses_list,path_to_analysis_sub_ses_list,force)
        for orig_config_json, copy_config_json in zip(parser_namespace.container_specific_config, path_to_analysis_container_specific_config):
            do.copy_file(orig_config_json, copy_config_json, force)    
        logger.debug(f'\n the analysis folder is {Dir_analysis}, all the cofigs has been copied') 
    
    if run_lc:
        logger.warning(f'\n RUN MODE, this is the analysis folder that we are going to run:\n {Dir_analysis}')
        # also copy the newest
        do.copy_file(parser_namespace.lc_config, path_to_analysis_lc_config, force) 
        do.copy_file(parser_namespace.sub_ses_list,path_to_analysis_sub_ses_list,force)
        for orig_config_json, copy_config_json in zip(parser_namespace.container_specific_config,path_to_analysis_container_specific_config):
            do.copy_file(orig_config_json, copy_config_json, force)    
        logger.debug(f'\n the analysis folder is {Dir_analysis}, all the configs has been copied')         
        
        copies = [path_to_analysis_lc_config, path_to_analysis_sub_ses_list] + path_to_analysis_container_specific_config
    
        all_copies_present= all(os.path.isfile(copy_path) for copy_path in copies)

        if all_copies_present:
            pass
        else:
            logger.error(f'\n did NOT detect back up configs in the analysis folder, Please check then continue the run mode')
    return Dir_analysis, path_to_analysis_container_specific_config

# %% prepare_input_files
def prepare_dwi_input(parser_namespace, Dir_analysis, lc_config, df_subSes, layout, path_to_analysis_container_specific_config):
    """

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
                +"---starting to prepare the input files for analysis\n")
    
    container = lc_config["general"]["container"]
    force = lc_config["general"]["force"]   
    run_lc = parser_namespace.run_lc    
    force= force or run_lc    
    # first thing, if the container specific config is not correct, then not doing anything
    if len(parser_namespace.container_specific_config)==0:
                logger.error("\n"
                              +"Input file error: the container specific config is not provided")
                raise FileNotFoundError("Didn't input container_specific_config, please indicate it in your command line flag -cc")
    else:
        version = lc_config["container_specific"][container]["version"]
        if "anatrois" in container or "freesurferator" in container:
            pre_fs = lc_config["container_specific"][container]["pre_fs"]
    
    # If freesurferator, before copying configs, existingFS and control input fields need to be in the config.json
    if container in "freesurferator":
        control_points = lc_config["container_specific"][container]["control_points"] #specific for freesurferator
        container_specific_config_data = json.load(open(parser_namespace.container_specific_config[0]))
        container_specific_config_data["inputs"] = {}
        container_config_inputs = container_specific_config_data["inputs"]
        # if pre_fs. add pre_fs in the inputs field of the container specific config.json, otherwise add T1.nii.gz
        if pre_fs:
            container_config_inputs["pre_fs"] = {'location': {'path': os.path.join('/flywheel/v0/input/pre_fs', 'existingFS.zip'), 'name': 'existingFS.zip'}, 'base': 'file'}
        else:
            container_config_inputs["anat"] = {'location': {'path': os.path.join('/flywheel/v0/input/anat', 'T1.nii.gz'), 'name': 'T1.nii.gz'}, 'base': 'file'}
        # add control_points in the inputs field of the container specific config.json 
        if control_points:
            container_config_inputs["control_points"] =  {'location': 
            {'path': '/flywheel/v0/input/control_points/control.dat', 'name': 'control.dat'}, 'base': 'file'}

        if len(lc_config["container_specific"][container]["mniroizip"]) != 0:
            container_config_inputs["mniroizip"] =  {'location': 
            {'path': '/flywheel/v0/input/mniroizip/mniroizip.zip', 'name': 'mniroizip.zip'}, 'base': 'file'}
        if len(lc_config["container_specific"][container]["annotfile"]) != 0:
            container_config_inputs["annotfile"] =  {'location': 
            {'path': '/flywheel/v0/input/annotfile/annotfile.zip', 'name': 'annotfile.zip'}, 'base': 'file'}        

        # Add the new options
        container_specific_config_data["inputs"] = container_config_inputs
        # Write the new config file
        with open(path_to_analysis_container_specific_config[0] , "w") as outfile:
            json.dump(container_specific_config_data, outfile, indent = 4)
        # TODO:
        # "t1w_anatomical_2": gear_context.get_input_path("t1w_anatomical_2"),
        # "t1w_anatomical_3": gear_context.get_input_path("t1w_anatomical_3"),
        # "t1w_anatomical_4": gear_context.get_input_path("t1w_anatomical_4"),
        # "t1w_anatomical_5": gear_context.get_input_path("t1w_anatomical_5"),
        # "t2w_anatomical": gear_context.get_input_path("t2w_anatomical"),
        
    # If freesurferator, before copying configs, existingFS and control input fields need to be in the config.json
    if container in "rtp2-preproc":
        container_specific_config_data = json.load(open(parser_namespace.container_specific_config[0]))
        container_specific_config_data["inputs"] = {}
        container_config_inputs = container_specific_config_data["inputs"]

        container_config_inputs["ANAT"] = {'location': {'path': os.path.join('/flywheel/v0/input/ANAT', 'T1.nii.gz'), 'name': 'T1.nii.gz'}, 'base': 'file'}
        container_config_inputs["BVAL"] = {'location': {'path': os.path.join('/flywheel/v0/input/BVAL', 'dwiF.bval'), 'name': 'dwiF.bval'}, 'base': 'file'}
        container_config_inputs["BVEC"] = {'location': {'path': os.path.join('/flywheel/v0/input/BVEC', 'dwiF.bvec'), 'name': 'dwiF.bvec'}, 'base': 'file'}
        container_config_inputs["DIFF"] = {'location': {'path': os.path.join('/flywheel/v0/input/DIFF', 'dwiF.nii.gz'), 'name': 'dwiF.nii.gz'}, 'base': 'file'}
        container_config_inputs["FSMASK"] = {'location': {'path': os.path.join('/flywheel/v0/input/FSMASK', 'brainmask.nii.gz'), 'name': 'brainmask.nii.gz'}, 'base': 'file'}
        container_specific_config_data["inputs"] = container_config_inputs
        
        with open(path_to_analysis_container_specific_config[0] , "w") as outfile:
            json.dump(container_specific_config_data, outfile, indent = 4)

    # If freesurferator, before copying configs, existingFS and control input fields need to be in the config.json
    if container in "rtp2-pipeline":
        container_specific_config_data = json.load(open(parser_namespace.container_specific_config[0]))
        container_specific_config_data["inputs"] = {}
        container_config_inputs = container_specific_config_data["inputs"]

        container_config_inputs["anatomical"] = {'location': {'path': os.path.join('/flywheel/v0/input/anatomical', 'T1.nii.gz'), 'name': 'T1.nii.gz'}, 'base': 'file'}
        container_config_inputs["bval"] = {'location': {'path': os.path.join('/flywheel/v0/input/bval', 'dwi.bval'), 'name': 'dwi.bval'}, 'base': 'file'}
        container_config_inputs["bvec"] = {'location': {'path': os.path.join('/flywheel/v0/input/bvec', 'dwi.bvec'), 'name': 'dwi.bvec'}, 'base': 'file'}
        container_config_inputs["dwi"] = {'location': {'path': os.path.join('/flywheel/v0/input/dwi', 'dwi.nii.gz'), 'name': 'dwi.nii.gz'}, 'base': 'file'}        
        container_config_inputs["fs"] = {'location': {'path': os.path.join('/flywheel/v0/input/fs', 'fs.zip'), 'name': 'fs.zip'}, 'base': 'file'}
        container_config_inputs["tractparams"] = {'location': {'path': os.path.join('/flywheel/v0/input/tractparams', 'tractparams.csv'), 'name': 'tractparams.csv'}, 'base': 'file'}
        
        with open(path_to_analysis_container_specific_config[0] , "w") as outfile:
            json.dump(container_specific_config_data, outfile, indent = 4)

        # "fsmask": gear_context.get_input_path("fsmask"),

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
                        
            tmpdir = os.path.join(
                Dir_analysis,
                "sub-" + sub,
                "ses-" + ses,
                "output", "tmp"
            )
            logdir = os.path.join(
                Dir_analysis,
                "sub-" + sub,
                "ses-" + ses,
                "output", "log"
            )

            if not os.path.isdir(tmpdir):
                os.makedirs(tmpdir)
            logger.info(f"\n the tmp dir is created at {tmpdir}, and it is {os.path.isdir(tmpdir)} that this file exists")
            if not os.path.isdir(logdir):
                os.makedirs(logdir)
            
            do.copy_file(parser_namespace.lc_config, os.path.join(logdir,'lc_config.yaml'), force) 
               
  

            if container in ["rtppreproc" ,"rtp2-preproc"]:
                do.copy_file(path_to_analysis_container_specific_config[0], os.path.join(logdir,'config.json'), force)
                dwipre.rtppreproc(parser_namespace, Dir_analysis, lc_config, sub, ses, layout)
            
            elif container in ["rtp-pipeline", "rtp2-pipeline"]:
                
                if not len(parser_namespace.container_specific_config) == 2:
                    logger.error("\n"
                              +f"Input file error: the RTP-PIPELINE config is not provided completely")
                    raise FileNotFoundError('The RTP-PIPELINE needs the config.json and tratparams.csv as container specific configs')
                
                do.copy_file(path_to_analysis_container_specific_config[0],os.path.join(logdir, "config.json"), force) 
                do.copy_file(path_to_analysis_container_specific_config[-1],os.path.join(logdir, "tractparams.csv"), force) 
                
                dwipre.rtppipeline(parser_namespace, Dir_analysis,lc_config, sub, ses, layout)
            
            elif container in "anatrois":
                logger.info('we do the anatrois')
                do.copy_file(parser_namespace.container_specific_config[0], os.path.join(logdir,'config.json'), force)
                dwipre.anatrois(parser_namespace, Dir_analysis,lc_config,sub, ses, layout)
            
            elif container in "freesurferator":
                logger.info('we do the freesurferator')
                do.copy_file(path_to_analysis_container_specific_config[0], os.path.join(logdir,'config.json'), force)
                dwipre.anatrois(parser_namespace, Dir_analysis,lc_config,sub, ses, layout)
                # in dwipre.anatrois the config.json can be modified if there are pre_fs and control.dat, so we neeed to copy the .json 
                # after editing it 
                

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

def fmriprep_intended_for(sub_ses_list, bidslayout):
    '''
    not implement yet, thinking how to smartly do the job
    '''
    layout= bidslayout
    #number_of_topups= fmriprep_configs['number_of_topups'] # a str
    #index_of_new_topups= fmriprep_configs['number_of_topups'] # a str about the functional run 
    exp_TRs= [2] #fmriprep_configs['exp_TRs'] # a list
    
    for row in sub_ses_list.itertuples(index=True, name="Pandas"):
        sub = row.sub
        ses = row.ses
        RUN = row.RUN
        func = row.func
        
        if RUN == "True" and func == "True":

            logger.info(f'\n working on {sub}...')

        
            # load func and fmaps
            funcNiftis = layout.get(subject=sub, session=ses, extension='.nii.gz', datatype='func')
            fmapNiftis = layout.get(subject=sub, session=ses, extension='.nii.gz', datatype='fmap')

            funcNiftisMeta = [funcNiftis[i].get_metadata() for i in range(len(funcNiftis))]
            fmapNiftisMeta = [fmapNiftis[i].get_metadata() for i in range(len(fmapNiftis))]

            for res in exp_TRs:
                funcN = np.array(funcNiftis)[[i['RepetitionTime'] == res for i in funcNiftisMeta]]
                # fmapN = np.array(fmapNiftis)[[i['RepetitionTime'] == res for i in fmapNiftisMeta]]
                fmapN = fmapNiftis
                
                # make list with all relative paths of func
                funcNiftisRelPaths = [path.join(*funcN[i].relpath.split("/")[1:]) for i in range(len(funcN))]
                funcNiftisRelPaths = [fp for fp in funcNiftisRelPaths if ((fp.endswith('_bold.nii.gz') or 
                                                                        fp.endswith('_sbref.nii.gz')) and 
                                                                        all([k not in fp for k in ['mag', 'phase']]))]

                # add list to IntendedFor field in fmap json
                for fmapNifti in fmapN:
                    if not path.exists(fmapNifti.filename.replace('.nii.gz', '_orig.json')):
                        f = fmapNifti.path.replace('.nii.gz', '.json')

                        with open(f, 'r') as file:
                            j = json.load(file)

                        j['IntendedFor'] = [f.replace("\\", "/") for f in funcNiftisRelPaths]

                        rename(f, f.replace('.json', '_orig.json'))

                        with open(f, 'w') as file:
                            json.dump(j, file, indent=2)
        
    '''add a function to check, if all the intended for is here, if so, return fmriprep'''
    
    return 

def link_vistadisplog(basedir, sub_ses_list, bids_layout):
    
    
    
    baseP=os.path.join(basedir,'BIDS','sourcedata','vistadisplog')

    
    for row in sub_ses_list.itertuples(index=True, name='Pandas'):
        sub  = row.sub
        ses  = row.ses
        RUN  = row.RUN
        func = row.func
        if RUN ==True and func == True:
            taskdict=  {}
            tasks= bids_layout.get_tasks(subject=sub, session=ses)
            for index, item in enumerate(tasks):
                taskdict[item]=1
                logger.debug(taskdict)
            matFiles = np.sort(glob(path.join(baseP, f'sub-{sub}', f'ses-{ses}', '20*.mat')))
            logger.debug(f"\n {path.join(baseP, f'sub-{sub}', f'ses-{ses}', '20*.mat')}")
            logger.debug(f'\n {matFiles}')
            for matFile in matFiles:

                stimName = loadmat(matFile, simplify_cells=True)['params']['loadMatrix']
                print(stimName)
                for key in taskdict:
                    logger.debug(key)
                    if key[2:] in stimName:
                        if 'tr-2' in stimName:
                            linkName = path.join(path.dirname(matFile), f'{sub}_{ses}_task-{key}_run-0{taskdict[key]}_params.mat')
                            
                            taskdict[key] += 1

                    if path.islink(linkName):
                        unlink(linkName)

                    symlink(path.basename(matFile), linkName)

    return True

def prepare_prf_input(basedir, container, config_path, sub_ses_list, bids_layout ,run_lc):
    # first prepare the sourcedata, the vistadisp-log
    # then write the subject information to the config.json file

    if not run_lc:
        # if the container is prfprepare, do the preparation for vistadisplog
        # copy the container specific information to the prfprepare.json.
        # copy the information in subseslist to the prfprepare.json
        # question, in this way, do we still need the config.json???
        sub_list=[]
        ses_list=[]
        for row in sub_ses_list.itertuples(index=True, name='Pandas'):
            sub  = row.sub
            ses  = row.ses
            RUN  = row.RUN
            func = row.func
            logger.debug(f'\n run is {RUN},type run is {type(RUN)} func is {func} --{sub}-{ses}' )
            if RUN == "True" and func == "True":    # i mean yes, but there will always to better options
                sub_list.append(sub)
                ses_list.append(ses)
        logger.debug(f'\nthis is sublist{sub_list}, and ses list {ses_list}\n')        
        with open(config_path, 'r') as config_json:
            j= json.load(config_json)
        
        if container == 'prfprepare':   
            # do i need to add a check here? I don't think so
            if link_vistadisplog(basedir,sub_ses_list, bids_layout):
                logger.info('\n'
                + f'the {container} prestep link vistadisplog has been done!')
                j['subjects'] =' '.join(sub_list)
                j['sessions'] =' '.join(ses_list)

        if container =='prfresult':    
            j['subjects'] =' '.join(sub_list)
            j['sessions'] =' '.join(ses_list)
        if container == 'prfanalyze-vista':
            j['subjectName'] =' '.join(sub_list)
            j['sessionName'] =' '.join(ses_list)
       
        
        with open(config_path, 'w') as config_json:
            json.dump(j, config_json, indent=2)
    return
