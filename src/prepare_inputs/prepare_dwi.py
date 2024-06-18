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
#%% import libraries

import os
import os.path as op
import re
import errno
import shutil
import sys
import nibabel as nib
import json
import subprocess as sp
import zipfile
import logging

from . import utils as do
from .utils import read_df


logger = logging.getLogger("Launchcontainers")


#%% the force version of create symlink
def force_symlink(file1, file2, force):
    """Creates symlinks making sure

    Args:
        file1 (str): path to the source file, which is the output of the previous container
        file2 (str): path to the destination file, which is the input of the current container
        force (bool): specifies if existing files will be rewritten or not. Set in the config.yaml file.

    Raises:
        n (OSError): Raised if input file does not exist when trying to create a symlink between file1 and file2 
        e (OSError):
        e: _description_
    """
    logger.info("\n"
               +"-----------------------------------------------\n")
    # If force is set to False (we do not want to overwrite)
    if not force:
        try:
            # Try the command, if the files are correct and the symlink does not exist, create one
            logger.info("\n"
                       +f"---creating symlink for source file: {file1} and destination file: {file2}\n")
            os.symlink(file1, file2)
            logger.info("\n"
                       +f"--- force is {force}, -----------------creating success -----------------------\n")
        # If raise [erron 2]: file does not exist, print the error and pass
        except OSError as n:
            if n.errno == 2:
                logger.error("\n"
                             +"***An error occured \n" 
                             +"input files are missing, please check \n")
                pass
            # If raise [errno 17] the symlink exist, we don't force and print that we keep the original one
            elif n.errno == errno.EEXIST:
                logger.warning("\n"+ 
                           f"--- force is {force}, symlink exist, remain old \n")
            else:
                logger.error("\n"+ "Unknown error, break the program")
                raise n

    # If we set force to True (we want to overwrite)
    if force:
        try:
            # Try the command, if the file are correct and symlink not exist, it will create one
            os.symlink(file1, file2)
            logger.info("\n"
                       +f"--- force is {force}, symlink empty, new link created successfully\n ")
        # If the symlink exists, OSError will be raised
        except OSError as e:
            if e.errno == errno.EEXIST:
                os.remove(file2)
                logger.info("\n"
                           +f"--- force is {force} but the symlink exists, unlinking\n ")
                os.symlink(file1, file2)
                logger.warning("\n"
                           +"--- overwriting the existing symlink")
                logger.info("\n"
                           +"----------------- Overwrite success -----------------------\n")
            elif e.errno == 2:
                logger.error("\n"
                             +"***input files are missing, please check that they exist\n")
                raise e
            else:
                logger.error("\n"
                           +"***ERROR***\n"
                           +"We do not know what happened\n")
                raise e
    logger.info("\n"
               +"-----------------------------------------------\n")
    return


#%% check if tractparam ROI was created in the anatrois fs.zip file
def check_tractparam(lc_config, sub, ses, tractparam_df):

    """Checks the correctness of the given parameters.

    Args:
        lc_config (dict): _description_
        sub (str): _description_
        ses (str): _description_
        tractparam_df (pandas.DataFrame): _description_
            inherited parameters: path to the fs.zip file, defined by lc_config, sub, ses.

    Raises:
        FileNotFoundError: _description_

    Returns:
        rois_are_there (bool): Whether the regions of interest (ROIs) are present or not
    """
    # Define the list of required ROIs
    logger.info("\n"+
                "#####################################################\n")
    roi_list=[]
    # Iterate over some defined roisand check if they are required or not in the config.yaml
    for col in ['roi1', 'roi2', 'roi3', 'roi4',"roiexc1","roiexc2"]:
        for val in tractparam_df[col][~tractparam_df[col].isna()]:
            if '_AND_' in val:
                multi_roi= val.split('_AND_')
                roi_list.extend(multi_roi)
            else:
                if val != "NO":                    
                    roi_list.append(val)
    
    required_rois= set(roi_list)

    # Define the zip file
    basedir = lc_config["general"]["basedir"]
    container = lc_config["general"]["container"]
    bidsdir_name= lc_config["general"]["bidsdir_name"]
    precontainer_anat = lc_config["container_specific"][container]["precontainer_anat"]
    anat_analysis_name = lc_config["container_specific"][container]["anat_analysis_name"]
    
    # Define where the fs.zip file is
    fs_zip = op.join(
        basedir,
        bidsdir_name,
        "derivatives",
        f'{precontainer_anat}',
        "analysis-" + anat_analysis_name,
        "sub-" + sub,
        "ses-" + ses,
        "output", "fs.zip"
    )
    
    # Extract .gz files from zip file and check if they are all present
    with zipfile.ZipFile(fs_zip, 'r') as zip:
        zip_gz_files = set(zip.namelist())
    
    # See which ROIs are present in the fs.zip file
    required_gz_files = set(f"fs/ROIs/{file}.nii.gz" for file in required_rois)
    logger.info("\n"
                +f"---The following are the ROIs in fs.zip file: \n {zip_gz_files} \n"
                +f"---there are {len(zip_gz_files)} .nii.gz files in fs.zip from anatrois output\n"
                +f"---There are {len(required_gz_files)} ROIs that are required to run RTP-PIPELINE\n")
    if required_gz_files.issubset(zip_gz_files):
        logger.info("\n"
                +"---checked! All required .gz files are present in the fs.zip \n")
    else:
        missing_files = required_gz_files - zip_gz_files
        logger.error("\n"
                     +f"*****Error: \n"
                     +f"there are {len(missing_files)} missed in fs.zip \n"
                     +f"The following .gz files are missing in the zip file:\n {missing_files}")
        raise FileNotFoundError("Required .gz file are missing")
    
    ROIs_are_there= required_gz_files.issubset(zip_gz_files)
    logger.info("\n"+
                "#####################################################\n")
    return ROIs_are_there

#%%
def anatrois(dict_store_cs_configs, analysis_dir,lc_config, sub, ses, layout,run_lc):
    
    """anatrois function creates symbolic links for the anatrois container

    Args:
        analysis_dir (_type_): directory to analyze
        lc_config (dict): the lc_config dictionary from _read_config
        sub (str): subject name
        ses (str): session name
        layout (_type_): _description_

    Raises:
        FileNotFoundError: _description_
        FileNotFoundError: _description_
    """

    # General level variables:
    basedir = lc_config["general"]["basedir"]
    container = lc_config["general"]["container"]    
    bidsdir_name=lc_config["general"]["bidsdir_name"]  
    # If force is False, then we don't want to overwrite anything
    # If force is true, and we didn't run_lc(in the prepare mode), we will do the overwrite and so on
    # If force is true and we do run_lc, then we will never overwrite
    force = (lc_config["general"]["force"])
    force = force and (not run_lc)

    # Container specific:
    pre_fs = lc_config["container_specific"][container]["pre_fs"]
    prefs_dir_name = lc_config["container_specific"][container]["prefs_dir_name"]
    prefs_analysis_name = lc_config["container_specific"][container]["prefs_analysis_name"]
    prefs_zipname = lc_config["container_specific"][container]["prefs_zipname"]
    annotfile = lc_config["container_specific"][container]["annotfile"]
    mniroizip = lc_config["container_specific"][container]["mniroizip"]
    
    # define input output folder for this container
    dstDir_input = op.join(
        analysis_dir,
        "sub-" + sub,
        "ses-" + ses,
        "input",
    )
    dstDir_output = op.join(
        analysis_dir,
        "sub-" + sub,
        "ses-" + ses,
        "output",
    )
    if container == "freesurferator":
        dstDir_work = op.join(
            analysis_dir,
            "sub-" + sub,
            "ses-" + ses,
            "work",
        )
        if not op.exists(dstDir_work):
            os.makedirs(dstDir_work)
    # create corresponding folder
    if op.exists(dstDir_input) and force:
        shutil.rmtree(dstDir_input)    
    if op.exists(dstDir_output) and force:        
        shutil.rmtree(dstDir_output)
    
    if not op.exists(dstDir_input):
        os.makedirs(dstDir_input)
    if not op.exists(dstDir_output):
        os.makedirs(dstDir_output)
 
    # specific for freesurferator
    if container == "freesurferator":
        control_points = lc_config["container_specific"][container]["control_points"]
        prefs_unzipname = lc_config["container_specific"][container]["prefs_unzipname"]   
    else:
        # if not running freesurferator control_points field is not available, then we set it False
        control_points = False
    
    # read json, this json is already written from previous preparasion step
    json_under_analysis_dir=dict_store_cs_configs['config_path']
    config_json_instance = json.load(open(json_under_analysis_dir))
    required_inputfiles=config_json_instance['inputs'].keys()
    
    # 5 main filed needs to be in anatrois if all specified, so there will be 5 checks
    if "anat" in required_inputfiles:
        src_path_anat_lst= layout.get(subject= sub, session=ses, extension='nii.gz',suffix= 'T1w',return_type='filename')
        if len(src_path_anat_lst) == 0:
            raise FileNotFoundError(f'the T1w.nii.gz you are specifying for sub-{sub}_ses-{ses} does NOT exist or the folder is not BIDS format, please check')
        else:
            src_path_anat = src_path_anat_lst[0]        
        dst_fname_anat=config_json_instance['inputs']['anat']['location']['name']
        dst_path_anat=op.join(dstDir_input, "anat", dst_fname_anat)
        
        if not op.exists(op.join(dstDir_input, "anat")):
            os.makedirs(op.join(dstDir_input, "anat"))
        force_symlink(src_path_anat, dst_path_anat, force)

    # If we ran freesurfer before:
    if "pre_fs" in required_inputfiles:   
        pre_fs_path = op.join(
            basedir,
            bidsdir_name,
            "derivatives",
            f'{prefs_dir_name}',
            "analysis-" + prefs_analysis_name,
            "sub-" + sub,
            "ses-" + ses,
            "output",
        )
        logger.info("\n"
                   +f"---the patter of fs.zip filename we are searching is {prefs_zipname}\n"
                   +f"---the directory we are searching for is {pre_fs_path}")
        logger.debug("\n"
                     +f'the tpye of patter is {type(prefs_zipname)}')
        zips=[]
        for filename in os.listdir(pre_fs_path):
            if filename.endswith(".zip") and re.match(prefs_zipname, filename):
                zips.append(filename)
                if "control_points" in required_inputfiles:
                    if op.isdir(op.join(pre_fs_path,filename)) and re.match(prefs_unzipname, filename):
                        src_path_ControlPoints = op.join(
                            pre_fs_path,
                            filename,
                            "tmp",
                            "control.dat"
                            )
                    else:
                        raise FileNotFoundError("Didn't found control_points .zip file")
                    
        if len(zips) == 0:
            logger.error("\n"+
                f"There are no files with pattern: {prefs_zipname} in {pre_fs_path}, we will listed potential zip file for you"
            )
            raise FileNotFoundError("pre_fs_path is empty, no previous analysis was found")
        elif len(zips) == 1:
            src_path_fszip = op.join(pre_fs_path,zips[0])            
        else:    
            zips_by_time = sorted(zips, key=op.getmtime)
            answer = input(
                f"Do you want to use the newset fs.zip: \n{zips_by_time[-1]} \n we get for you? \n input y for yes, n for no"
            )
            if answer in "y":
                src_path_fszip = zips_by_time[-1]
            else:
                logger.error("\n"+"An error occurred"
                            +zips_by_time +"\n" # type: ignore
                            +"no target preanalysis.zip file exist, please check the config_lc.yaml file")
                sys.exit(1)
        
        dst_fname_fs=config_json_instance['inputs']['pre_fs']['location']['name']
        dst_path_fszip=op.join(dstDir_input, "pre_fs", dst_fname_fs)        
        if not op.exists(op.join(dstDir_input, "pre_fs")):
            os.makedirs(op.join(dstDir_input, "pre_fs"))
        force_symlink(src_path_fszip, dst_path_fszip, force)  
        
        if "control_points" in required_inputfiles:
            dst_fname_cp=config_json_instance['inputs']['control_points']['location']['name']
            dst_path_cp=op.join(dstDir_input, "pre_fs", dst_fname_cp)                 
            if not op.exists(op.join(dstDir_input,"control_points")):
                os.makedirs(op.join(dstDir_input,"control_points"))       
            force_symlink(src_path_ControlPoints, dst_path_cp, force) 
              
    if "annotfile" in required_inputfiles:
        
        fname_annot=config_json_instance['inputs']['annotfile']['location']['name']
        src_path_annot = op.join(analysis_dir, fname_annot)
        dst_path_annot=op.join(dstDir_input, "annotfile", fname_annot)  
        
        if not op.exists(op.join(dstDir_input, "annotfile")):
            os.makedirs(op.join(dstDir_input, "annotfile"))
        force_symlink(src_path_annot, dst_path_annot, force)
    
    if "mniroizip" in required_inputfiles:
        
        fname_mniroi=config_json_instance['inputs']['mniroizip']['location']['name']
        src_path_mniroi = op.join(analysis_dir, fname_mniroi)
        dst_path_mniroi=op.join(dstDir_input, "mniroizip", fname_mniroi)  
        
        if not op.exists(op.join(dstDir_input, "mniroizip")):
            os.makedirs(op.join(dstDir_input, "mniroizip"))
        force_symlink(src_path_mniroi, dst_path_mniroi, force)


    logger.info("\n"
               +"-----------------The symlink created-----------------------\n")
   
    return 
   

#%%
def rtppreproc(parser_namespace, analysis_dir, lc_config, sub, ses, layout):
    """
    Parameters
    ----------
    parser_namespace: parser obj
        it contains all the input argument in the parser

    lc_config : dict
        the lc_config dictionary from _read_config
    sub : str
        the subject name looping from df_subSes
    ses : str
        the session name looping from df_subSes.
    
    Returns
    -------
    none, create symbolic links

    """

    # define local variables from lc_config dict
    # input from get_parser
    container_specific_config_path= parser_namespace.container_specific_config
    run_lc=parser_namespace.run_lc
    
    # general level variables:
    
    basedir = lc_config["general"]["basedir"]
    container = lc_config["general"]["container"]
    bidsdir_name=lc_config["general"]["bidsdir_name"]  
    force = (lc_config["general"]["force"])
    # if force is False, then we don't want to overwrite anything
    # if force is true, and we didn't run_lc(in the prepare mode), we will do the overwrite and so on
    # if force is true and we do run_lc, then we will never overwrite
    force=force and (not run_lc)
    # container specific:
    precontainer_anat = lc_config["container_specific"][container]["precontainer_anat"]
    anat_analysis_name = lc_config["container_specific"][container]["anat_analysis_name"]
    rpe = lc_config["container_specific"][container]["rpe"]
    version = lc_config["container_specific"][container]["version"]
    
    srcFile_container_config_json= container_specific_config_path[0]
    

    container_specific_config_data = json.load(open(srcFile_container_config_json))
    # if version =='1.2.0-3.0.3':
    #     phaseEnco_direc = container_specific_config_data["config"]["pe_dir"]
    # if version =='1.1.3':
    #     phaseEnco_direc = container_specific_config_data["config"]["acqd"]
    if container == "rtp2-preproc":
        phaseEnco_direc = container_specific_config_data["config"]["pe_dir"]
    if container == "rtppreproc":
        phaseEnco_direc = container_specific_config_data["config"]["acqd"]
    # the source directory that stores the output of previous anatrois analysis
    srcDirFs = op.join(
        basedir,
        bidsdir_name,
        "derivatives",
        f'{precontainer_anat}',
        "analysis-" + anat_analysis_name,
        "sub-" + sub,
        "ses-" + ses,
        "output",
    )

    # define the source file, this is where symlink will point to
    # T1 file in anatrois output
    srcFileT1 = op.join(srcDirFs, "T1.nii.gz")
    # brain mask file in anatrois output
    logger.debug(f'\n the precontainer_ana is {precontainer_anat}')
    if int(precontainer_anat.split('.')[1])<6: 
        srcFileMask = op.join(srcDirFs, "brainmask.nii.gz")
    if int(precontainer_anat.split('.')[1])>5: 
        srcFileMask = op.join(srcDirFs, "brain.nii.gz")
    
    # 3 dwi file that needs to be preprocessed, under BIDS/sub/ses/dwi
    # the nii
    srcFileDwi_nii = layout.get(subject= sub, session=ses, extension='nii.gz',suffix= 'dwi', direction=phaseEnco_direc, return_type='filename')[0]
    # the bval
    srcFileDwi_bval = layout.get(subject= sub, session=ses, extension='bval',suffix= 'dwi', direction=phaseEnco_direc, return_type='filename')[0]
    # the bve
    srcFileDwi_bvec =layout.get(subject= sub, session=ses, extension='bvec',suffix= 'dwi',direction=phaseEnco_direc, return_type='filename')[0]
    
    # check how many *dir_dwi.nii.gz there are in the BIDS/sub/ses/dwi directory
    phaseEnco_direc_dwi_files = layout.get(subject= sub, session=ses, extension='nii.gz',suffix= 'dwi', direction=phaseEnco_direc, return_type='filename')
    
    if len(phaseEnco_direc_dwi_files) > 1:
        dwi_acq = [f for f in phaseEnco_direc_dwi_files if 'acq-' in f]
        if len(dwi_acq) == 0:
            logger.warning("\n"
                       +f"No files with different acq- to concatenate.\n")
        elif len(dwi_acq) == 1:
            logger.warning("\n"
                       +f"Found only {dwi_acq[0]} to concatenate. There must be at least two files with different acq.\n")
        elif len(dwi_acq) > 1:
            if not op.isfile(srcFileDwi_nii):
                logger.info("\n"
                           +f"Concatenating with mrcat of mrtrix3 these files: {dwi_acq} in: {srcFileDwi_nii} \n")
                dwi_acq.sort()
                sp.run(['mrcat',*dwi_acq,srcFileDwi_nii])
            # also get the bvecs and bvals
            bvals_dir = layout.get(subject= sub, session=ses, extension='bval',suffix= 'dwi', direction=phaseEnco_direc, return_type='filename')
            bvecs_dir = layout.get(subject= sub, session=ses, extension='bvec',suffix= 'dwi', direction=phaseEnco_direc, return_type='filename')
            bvals_acq = [f for f in bvals_dir if 'acq-' in f]
            bvecs_acq = [f for f in bvecs_dir if 'acq-' in f]
            if len(dwi_acq) == len(bvals_acq) and not op.isfile(srcFileDwi_bval):
                bvals_acq.sort()
                bval_cmd = "paste -d ' '"
                for bvalF in bvals_acq:
                    bval_cmd = bval_cmd+" "+bvalF
                bval_cmd = bval_cmd+" > "+srcFileDwi_bval
                sp.run(bval_cmd,shell=True)
            else:
                logger.warning("\n"
                           +"Missing bval files")
            if len(dwi_acq) == len(bvecs_acq) and not op.isfile(srcFileDwi_bvec):
                bvecs_acq.sort()
                bvec_cmd = "paste -d ' '"
                for bvecF in bvecs_acq:
                    bvec_cmd = bvec_cmd+" "+bvecF
                bvec_cmd = bvec_cmd+" > "+srcFileDwi_bvec
                sp.run(bvec_cmd,shell=True)
            else:
                logger.warning("\n"
                           +"Missing bvec files")
    # check_create_bvec_bval（force) one of the todo here
    if rpe:
        if phaseEnco_direc == "PA":
            rpe_dir = "AP"
        elif phaseEnco_direc == "AP":
            rpe_dir = "PA"
        
        # the reverse direction nii.gz
        srcFileDwi_nii_R = layout.get(subject= sub, session=ses, extension='nii.gz',suffix= 'dwi', direction=rpe_dir, return_type='filename')[0]
        
        # the reverse direction bval
        srcFileDwi_bval_R_lst= layout.get(subject= sub, session=ses, extension='bval',suffix= 'dwi', direction=rpe_dir, return_type='filename')
        
        if len(srcFileDwi_bval_R_lst)==0:
            srcFileDwi_bval_R = srcFileDwi_nii_R.replace("dwi.nii.gz", "dwi.bval")
            logger.warning(f"\n the bval Reverse file are not find by BIDS, create empty file !!!")
        else:    
            srcFileDwi_bval_R = layout.get(subject= sub, session=ses, extension='bval',suffix= 'dwi', direction=rpe_dir, return_type='filename')[0]
        
        # the reverse direction bvec
        srcFileDwi_bvec_R_lst= layout.get(subject= sub, session=ses, extension='bvec',suffix= 'dwi', direction=rpe_dir, return_type='filename')
        if len(srcFileDwi_bvec_R_lst)==0:
            srcFileDwi_bvec_R = srcFileDwi_nii_R.replace("dwi.nii.gz", "dwi.bvec")
            logger.warning(f"\n the bvec Reverse file are not find by BIDS, create empty file !!!")       
        else:
            srcFileDwi_bvec_R =layout.get(subject= sub, session=ses, extension='bvec',suffix= 'dwi', direction=rpe_dir, return_type='filename')[0]

        # If bval and bvec do not exist because it is only b0-s, create them
        # (it would be better if dcm2niix would output them but...)
        # build the img matrix according to the shape of nii.gz
        img = nib.load(srcFileDwi_nii_R) # type: ignore
        volumes = img.shape[3] # type: ignore
        # if one of the bvec and bval are not there, re-write them
        if (not op.isfile(srcFileDwi_bval_R)) or (not op.isfile(srcFileDwi_bvec_R)):
            # Write bval file
            f = open(srcFileDwi_bval_R, "x")
            f.write(volumes * "0 ")
            f.close()
            logger.warning(f"\n Finish writing the bval Reverse file with all 0 !!!")
            # Write bvec file
            f = open(srcFileDwi_bvec_R, "x")
            f.write(volumes * "0 ")
            f.write("\n")
            f.write(volumes * "0 ")
            f.write("\n")
            f.write(volumes * "0 ")
            f.write("\n")
            f.close()
            logger.warning(f"\n Finish writing the bvec Reverse file with all 0 !!!")
    # create input and output directory for this container, the dstDir_output should be empty, the dstDir_input should contains all the symlinks
    dstDir_input = op.join(
        analysis_dir,
        "sub-" + sub,
        "ses-" + ses,
        "input",
    )
    dstDir_output = op.join(
        analysis_dir,
        "sub-" + sub,
        "ses-" + ses,
        "output",
    )

    if not op.exists(dstDir_input):
        os.makedirs(dstDir_input)
    if not op.exists(dstDir_output):
        os.makedirs(dstDir_output)
    # destination directory under dstDir_input
    if not op.exists(op.join(dstDir_input, "ANAT")):
        os.makedirs(op.join(dstDir_input, "ANAT"))
    if not op.exists(op.join(dstDir_input, "FSMASK")):
        os.makedirs(op.join(dstDir_input, "FSMASK"))
    if not op.exists(op.join(dstDir_input, "DIFF")):
        os.makedirs(op.join(dstDir_input, "DIFF"))
    if not op.exists(op.join(dstDir_input, "BVAL")):
        os.makedirs(op.join(dstDir_input, "BVAL"))
    if not op.exists(op.join(dstDir_input, "BVEC")):
        os.makedirs(op.join(dstDir_input, "BVEC"))
    if rpe:
        if not op.exists(op.join(dstDir_input, "RDIF")):
            os.makedirs(op.join(dstDir_input, "RDIF"))
        if not op.exists(op.join(dstDir_input, "RBVL")):
            os.makedirs(op.join(dstDir_input, "RBVL"))
        if not op.exists(op.join(dstDir_input, "RBVC")):
            os.makedirs(op.join(dstDir_input, "RBVC"))

    # Create the destination paths
    dstT1file = op.join(dstDir_input, "ANAT", "T1.nii.gz")
    dstMaskFile = op.join(dstDir_input, "FSMASK", "brainmask.nii.gz")

    dstFileDwi_nii = op.join(dstDir_input, "DIFF", "dwiF.nii.gz")
    dstFileDwi_bval = op.join(dstDir_input, "BVAL", "dwiF.bval")
    dstFileDwi_bvec = op.join(dstDir_input, "BVEC", "dwiF.bvec")

    if rpe:
        dstFileDwi_nii_R = op.join(dstDir_input, "RDIF", "dwiR.nii.gz")
        dstFileDwi_bval_R = op.join(dstDir_input, "RBVL", "dwiR.bval")
        dstFileDwi_bvec_R = op.join(dstDir_input, "RBVC", "dwiR.bvec")

    
    
    # Create the symbolic links
    force_symlink(srcFileT1, dstT1file, force)
    force_symlink(srcFileMask, dstMaskFile, force)
    force_symlink(srcFileDwi_nii, dstFileDwi_nii, force)
    force_symlink(srcFileDwi_bval, dstFileDwi_bval, force)
    force_symlink(srcFileDwi_bvec, dstFileDwi_bvec, force)
    logger.info("\n"
               +"-----------------The rtppreproc symlinks created\n")
    if rpe:
        force_symlink(srcFileDwi_nii_R, dstFileDwi_nii_R, force)
        force_symlink(srcFileDwi_bval_R, dstFileDwi_bval_R, force)
        force_symlink(srcFileDwi_bvec_R, dstFileDwi_bvec_R, force)
        logger.info("\n"
                   +"---------------The rtppreproc rpe=True symlinks created")
    return 



#%%
def rtppipeline(parser_namespace, analysis_dir,lc_config,sub, ses, layout):
    """"""
    
    """
    Parameters
    ----------
    lc_config : dict
        the lc_config dictionary from _read_config
    sub : str
        the subject name looping from df_subSes
    ses : str
        the session name looping from df_subSes.
    container_specific_config_path : str
        
    Returns
    -------
    none, create symbolic links

    """
    # define local variables from config dict
    # input from get_parser


    run_lc=parser_namespace.run_lc
    
    # general level variables:
    basedir = lc_config["general"]["basedir"]
    container = lc_config["general"]["container"]
    bidsdir_name = lc_config["general"]["bidsdir_name"]  
    force = (lc_config["general"]["force"])
    force = force and (not run_lc)

    # rtppipeline specefic variables
    precontainer_anat = lc_config["container_specific"][container]["precontainer_anat"]
    anat_analysis_name = lc_config["container_specific"][container]["anat_analysis_name"]
    precontainer_preproc = lc_config["container_specific"][container]["precontainer_preproc"]
    preproc_analysis_num = lc_config["container_specific"][container]["preproc_analysis_name"]
    # There is a bug before, when create symlinks the full path of trachparams are not passed, very weired
    srcFile_tractparams= op.join(analysis_dir, "tractparams.csv")

    # The source directory
    srcDirfs = op.join(
        basedir,
        bidsdir_name,
        "derivatives",
                    f'{precontainer_anat}',
        "analysis-" + anat_analysis_name,
        "sub-" + sub,
        "ses-" + ses,
        "output",
    )
    srcDirpp = op.join(
        basedir,
        bidsdir_name,
        "derivatives",
        precontainer_preproc,
        "analysis-" + preproc_analysis_num,
        "sub-" + sub,
        "ses-" + ses,
        "output",
    )
    # The source file
    srcFileT1 = op.join(srcDirpp, "t1.nii.gz")
    srcFileFs = op.join(srcDirfs, "fs.zip")
    srcFileDwi_bvals = op.join(srcDirpp, "dwi.bvals")
    srcFileDwi_bvec = op.join(srcDirpp, "dwi.bvecs")
    srcFileDwi_nii = op.join(srcDirpp, "dwi.nii.gz")

    # Create input and output directory for this container, 
    # the dstDir_output should be empty, the dstDir_input should contains all the symlinks
    dstDir_input = op.join(
        analysis_dir,
        "sub-" + sub,
        "ses-" + ses,
        "input",
    )
    dstDir_output = op.join(
        analysis_dir,
        "sub-" + sub,
        "ses-" + ses,
        "output",
    )

    # under dstDir_input there are a lot of dir also needs to be there to have symlinks
    if not op.exists(dstDir_input):
        os.makedirs(dstDir_input)
    if not op.exists(dstDir_output):
        os.makedirs(dstDir_output)
    if not op.exists(op.join(dstDir_input, "anatomical")):
        os.makedirs(op.join(dstDir_input, "anatomical"))
    if not op.exists(op.join(dstDir_input, "fs")):
        os.makedirs(op.join(dstDir_input, "fs"))
    if not op.exists(op.join(dstDir_input, "dwi")):
        os.makedirs(op.join(dstDir_input, "dwi"))
    if not op.exists(op.join(dstDir_input, "bval")):
        os.makedirs(op.join(dstDir_input, "bval"))
    if not op.exists(op.join(dstDir_input, "bvec")):
        os.makedirs(op.join(dstDir_input, "bvec"))
    if not op.exists(op.join(dstDir_input, "tractparams")):
        os.makedirs(op.join(dstDir_input, "tractparams"))

    # Create the destination file
    dstAnatomicalFile = op.join(dstDir_input, "anatomical", "T1.nii.gz")
    dstFsfile = op.join(dstDir_input, "fs", "fs.zip")
    dstDwi_niiFile = op.join(dstDir_input, "dwi", "dwi.nii.gz")
    dstDwi_bvalFile = op.join(dstDir_input, "bval", "dwi.bval")
    dstDwi_bvecFile = op.join(dstDir_input, "bvec", "dwi.bvec")
    dst_tractparams = op.join(dstDir_input, "tractparams", "tractparams.csv")

    dstFile_tractparams = op.join(analysis_dir, "tractparams.csv")

    # the tractparams check, at the analysis folder 
    tractparam_df,_ =read_df(dstFile_tractparams)
    check_tractparam(lc_config, sub, ses, tractparam_df)


    # Create the symbolic links
    force_symlink(srcFileT1, dstAnatomicalFile, force)
    force_symlink(srcFileFs, dstFsfile, force)
    force_symlink(srcFileDwi_nii, dstDwi_niiFile, force)
    force_symlink(srcFileDwi_bvec, dstDwi_bvecFile, force)
    force_symlink(srcFileDwi_bvals, dstDwi_bvalFile, force)
    force_symlink(srcFile_tractparams, dst_tractparams, force)
    logger.info("\n"
               +"-----------------The rtppipeline symlinks created\n")
    return 
    
