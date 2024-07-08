import os
import os.path as op
import shutil

def format_onset_file():
    
    return

def move_onset_files_to_bids(lc_config,l1_glm_yaml,sub,ses):
    basedir=lc_config["general"]["basedir"]
    bidsdir_name=lc_config["general"]["bidsdir_name"]
    onsetdir_name=lc_config["container_specific"]["l1_glm"]["onsetdir"]
    onsetdir=op.join(basedir,onsetdir_name)
    bidsdir=op.join(basedir,bidsdir_name)

    task=l1_glm_yaml["experiment"]["task"]
    runs=l1_glm_yaml["experiment"]["run_nums"]
    for runnum in runs:
        src_fname=f'sub-{sub}_ses-{ses}_task-{task}_run-{runnum}_event.tsv'
        target_fname=f'sub-{sub}_ses-{ses}_task-{task}_run-{runnum}_events.tsv'
        src_onset=op.join(onsetdir,src_fname)
        target_path=op.join(bidsdir,f'sub-{sub}',f'ses-{ses}','func')
        if not op.exists(target_path):
            os.makedirs(target_path)
        
        target=op.join(bidsdir,f'sub-{sub}',f'ses-{ses}','func',target_fname)
        if os.path.exists(target):
            os.remove(target)
        try:
            shutil.copy(src_onset,target)
        except:
            continue
    return


def smooth_time_series(subject, session, l1_glm_yaml, lc_config):
    # get the variables for input and output files
    basedir=lc_config['general']['basedir']
    bidsdir_name=lc_config['general']['bidsdir_name']
    fmriprep_dir_name=lc_config['container_specific']['fmri_glm']['fmriprep_dir_name']
    fmriprep_ana_name=lc_config['container_specific']['fmri_glm']['fmriprep_ana_name']    
    fmriprep_dir=op.join(basedir,bidsdir_name,'derivatives',fmriprep_dir_name,fmriprep_ana_name)
    
    task=l1_glm_yaml["experiment"]["task"]
    # set up freesurfer environment

    # generate the cmd 
    #gii_in=f"{fmriprep_dir}/sub-{subject}/ses-{session}/func/sub-{subject}_ses-{session}_task-{task}_run-{run}_hemi-{hemi}_space-{space}_bold.func.gii"
    #gii_out=f"{fmriprep_dir}/sub-{subject}/ses-{session}/func/sub-{subject}_ses-{session}_task-{task}_run-{run}_hemi-{hemi}_space-{space}_desc-smoothed0${time_series_smooth_kernel}_bold.func.gii"
		
    #cmd=f"mris_fwhm --i {gii_in} --o {gii_out} --so --fwhm {time_series_smooth_kernel} --subject sub-{subject} --hemi {hemi} "
    cmd=None
    return cmd