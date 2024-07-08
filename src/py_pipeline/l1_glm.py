# Python script for doing first lvl fMRI analysis
# features 3:
#   1. take fMRIPrep output time series 
#   2. Can do time series smoothing for you
#   3. Can do parallel processing and takes little time

# input: 
#   config.yaml specifying input folder and whether do smooth or not
#   subseslist.txt specifying the subject, session, that will be processed
#   l1_analysis.json specifying the contrast you will use
#               task:
#               run_number:
#               freesurfer label:
#               output stats:
#               TODO: there are other analysis specific stuff like HRF, and how many regressor to include, but now will just go with the one I am using

# sturcture of the script
'''
def prepare_onset():
def do_smooth():
    Take input from config.yaml to get the orig time series
    Then call freesurfer smooth 

    input: from config.yaml to get folder, from config.json to get filename
    output: add time_series_smooth_kernel in the filename
def do_glm():

def main():
    take input from yaml and json and pass to do_smooth() and do_glm
    
    take input from subseslist to setup dask
    
    use dask to generate job script and call do_glm

    future=client.map(do_glm )

QA:   set time in the code, and monitor the time of each step
Logger: get logger and store the output into a txt
'''
import os.path as op
from os import makedirs
from nilearn.surface import load_surf_data
import numpy as np
from scipy import stats
from nilearn.glm.first_level import (
    make_first_level_design_matrix,
    first_level_from_bids,
)

from nilearn.glm.first_level.first_level import run_glm
from nilearn.glm.contrasts import compute_contrast
import pandas as pd
import nibabel as nib
import logging
import argparse
from argparse import RawDescriptionHelpFormatter
import sys
sys.path.append(op.abspath(op.join(op.dirname(__file__), '../../')))
# Now you can import utils
import utils as do
logger = logging.getLogger("Launchcontainers")

def mask_nii(mask_method, mask, source_nii):
    masked_nii=None
    return masked_nii

def save_statmap_to_gifti(data, outname):
    """Save a statmap to a gifti file.
    data: nilearn contrast model output, e.g., contrast.effect_size()
    outname: output file name
    """
    gii_to_save = nib.gifti.gifti.GiftiImage()
    gii_to_save.add_gifti_data_array(
        nib.gifti.gifti.GiftiDataArray(data=data, datatype="NIFTI_TYPE_FLOAT32")
    )
    nib.save(gii_to_save, outname)

def run_l1_glm(subject, session, lc_config, l1_glm_yaml):
    # (subject, session, fp_ana_name, output_name, slice_time_ref=0.5, use_smoothed=False, time_series_smooth_kernel=None)
    #subject= subject_sessions['BIDS_sub']
    #session= subject_sessions['BIDS_ses']
    # use_smooth: either False 0 or True 01 02 03 04 05 010
    ####
    #####
    #for debug
    # subject='05'
    # session='day2VA'
    # slice_time_ref=0.5
    # fp_ana_name='analysis-okazaki_ST05' 
    # output_name='analysis-testGetL1'
    # use_smoothed=False
    ####
    basedir=lc_config['general']['basedir']
    bidsdir_name=lc_config['general']['bidsdir_name']
    bids = op.join(basedir,bidsdir_name)  # op to BIDS root
    container=lc_config['general']['container']
    version=lc_config['container_specific'][container]['version']
    analysis_name=lc_config['general']['analysis_name']

    fmriprep_dir_name=lc_config['container_specific'][container]['fmriprep_dir_name']
    fmriprep_ana_name=lc_config['container_specific'][container]['fmriprep_ana_name']
    fmriprep_dir = op.join(
        "derivatives", fmriprep_dir_name , f'analysis-{fmriprep_ana_name}'
    )  # BIDS-relative path to fMRIPrep
    # get the freesurfer dir
    # default freesurer dir is the same as fmriprep, but it could be different
    pre_fs=lc_config['container_specific'][container]['pre_fs']
    if not pre_fs:
        fs_dir= op.join(fmriprep_dir,'sourcedata','freesurfer')
    if pre_fs:
        pre_fs_full_path=lc_config['container_specific'][container]['pre_fs_full_path']
        fs_dir= pre_fs_full_path

    use_smoothed=lc_config['container_specific'][container]['use_smoothed']
    time_series_smooth_kernel=lc_config['container_specific'][container]['time_series_smooth_kernel']

    task = l1_glm_yaml['experiment']['task']  # Task name
    run_nums = l1_glm_yaml['experiment']['run_nums']  # Runs to process
    dummy_scans = l1_glm_yaml['experiment']['dummy_scans']  # dummy scans at the beginning of the functional acquisition
    
    space = l1_glm_yaml['model']['space']  # BOLD projected on subject's freesurfer surface
    hemis = l1_glm_yaml['model']['hemis']     #, "R"]  # L for left, R for right
    logger.info(f"input hemis are {hemis}")
    mask_EPI =l1_glm_yaml['model']['mask_EPI']
    
    if mask_EPI:
        mask_method =l1_glm_yaml['model']['mask_method']
        if mask_method=="fslabel":
            fslabel_name =l1_glm_yaml['model']['fslabel_name']
            label_dir=op.join(fs_dir, f'sub-{subject}','label') 
        elif mask_method=='bimap.nii':
            bimap_nii_path =l1_glm_yaml['model']['bimap_nii_path']
            # the default location is the folder
            if len(bimap_nii_path.split('/'))==1:
                bimap_nii_path=op.join(bids, "derivatives",container,analysis_name,bimap_nii_path)
            

    fslabel_name=l1_glm_yaml['model']['fslabel_name']  
    slice_time_ref = l1_glm_yaml['model']['slice_time_ref']  
    hrf_model= l1_glm_yaml['model']['hrf_model'] 
    drift_model= l1_glm_yaml['model']['drift_model'] 
    drift_order= l1_glm_yaml['model']['drift_order']
    high_pass= l1_glm_yaml['model']['high_pass']    
    motion_regressors = l1_glm_yaml['model']['motion_regressors']
    use_acompcor=l1_glm_yaml['model']['use_acompcor']
    use_non_steady_state=l1_glm_yaml['model']['use_non_steady_state']
    use_consine_regressors=l1_glm_yaml['model']['use_consine_regressors']

    ### Define output directory
    analysis_name=lc_config['general']['analysis_name']
    outdir = op.join(bids, "derivatives",f'{container}_{version}',f'analysis-{analysis_name}', f'sub-{subject}',f'ses-{session}')

    if not op.exists(outdir):
        makedirs(outdir)


    
    ### Loop across hemispheres
    for hemi in hemis:
        print("Processing hemi", hemi)
        if hemi == "lh":
            hm = "L"
        if hemi == "rh":
            hm= "R"
        ### Final output dictionary for GLM contrast results (to be combined across runslater)
        contrast_objs = {}
        gii_allrun=[]
        frame_time_allrun=[]
        events_allrun=[]
        confounds_allrun=[]
        store_l1=[]
        ### Loop over runs
        for idx, run_num in enumerate(run_nums):
            print("Processing run", run_num)

            ### Load GIFTI data and z-score it
            run = (
                "run-" + run_num
            )  # Run string in filename (define as empty string "" if no run label)
            func_name = (
                f"sub-{subject}_ses-{session}_task-{task}_{run}_hemi-{hm}_space-{space}_bold.func.gii"
            )
            # If you smoothed data beforehand, make sure to point this to your smoothed file name!
            print(f"smooth is {use_smoothed}")
            if use_smoothed:
                func_name = func_name.replace("_bold", f"_desc-smoothed{time_series_smooth_kernel}_bold")
            
            nii_path = op.join(bids, fmriprep_dir, f'sub-{subject}', f"ses-{session}" ,"func", func_name)
            gii_data = load_surf_data(nii_path)
            
            # remove the dummy scans of all runs and then concat them 
            gii_data_float=np.vstack(gii_data[:,:]).astype(float)
            gii_remove_dummy=gii_data_float[:,dummy_scans::]
            gii_data_std = stats.zscore(gii_remove_dummy, axis=1)

        
            # # freesurfer label file
            # label_path=(f'{label_dir}/lh.votcnov1v2.label')
            # mask_votc= load_surf_data(label_path)
            
            
            # ### Get shape of data
            n_vertices = np.shape(gii_data_std)[0]
            n_scans = np.shape(gii_data_std)[1]

            if mask_EPI:
                if mask_method=='fslabel':
                    label_full_path=f'{label_dir}/{hemi}.{fslabel_name}.label'       
                    label=load_surf_data(label_full_path)
                    mask=np.zeros((n_vertices,1))
                    mask[label]=1
                elif mask_method=='bimap.nii':
                    # to do hehe
                    pass
            # mask the gii according to the yaml
            if mask_EPI:
                gii_data_std_masked=gii_data_std*mask
                gii_data_float_masked=gii_data_float*mask
                # I dont know how to use this, it seems a binary mask
                # gii_data_std_masked=nilearn.masking.apply_mask(gii_data_std, mask_votc, dtype='f', smoothing_fwhm=None, ensure_finite=True)            
                gii_allrun.append(gii_data_std_masked)
            else:
                gii_allrun.append(gii_data_std)

            ### Use the volumetric data just to get the events and confounds file           
            img_filters = [("desc", "preproc")]
            # specify session 
            img_filters.append(("ses", session))
            # If multiple runs are present, then add the run number to filter to specify
            if len(run) > 0:
                img_filters.append(("run", run_num))
            l1 = first_level_from_bids(
                bids,
                task,
                space_label="T1w",
                sub_labels=[subject],
                slice_time_ref=slice_time_ref,
                hrf_model=hrf_model,
                drift_model=drift_model,  # Do not high_pass since we use fMRIPrep's cosine regressors
                drift_order=drift_order,  # Do not high_pass since we use fMRIPrep's cosine regressors
                high_pass=high_pass,  # Do not high_pass since we use fMRIPrep's cosine regressors
                img_filters=img_filters,
                derivatives_folder=fmriprep_dir,
            )

            ### Extract information from the prepared model
            t_r = l1[0][0].t_r
            events = l1[2][0][0]  # Dataframe of events information
            print(l1)
            confounds = l1[3][0][0]  # Dataframe of confounds
            
            # get rid of rest so that the setting would be the same as spm
            events_nobaseline=events[events.loc[:,'trial_type']!='rest']
            events_nobaseline.loc[:,'onset']=events_nobaseline['onset']+idx*(n_scans)*t_r
            
            events_allrun.append(events_nobaseline)
            store_l1.append(l1)
            ### From the confounds file, extract only those of interest
            # Start with the motion and acompcor regressors
            motion_keys = motion_regressors
            # Get ACompCor components (all to explain 50% variance)
            
            a_compcor_keys = [key for key in confounds.keys() if "a_comp_cor" in key]

            # Now add non-steady-state volumes
            non_steady_state_keys = [key for key in confounds.keys() if "non_steady" in key]

            # Add cosine regressors which act to high-pass filter data at 1/128 Hz
            cosine_keys = [key for key in confounds.keys() if "cosine" in key]

            # Pull out the confounds we want to keep
            confound_keys_keep = (
                motion_keys + a_compcor_keys + cosine_keys + non_steady_state_keys
            )
            confounds_keep = confounds[confound_keys_keep]

            # Set first value of FD column to the column mean
            confounds_keep["framewise_displacement"][0] = np.nanmean(
                confounds_keep["framewise_displacement"]
            )
            confounds_keep=confounds_keep.iloc[6:]
            confounds_allrun.append(confounds_keep)
            ### Create the design matrix
            # Start by getting times of scans
            frame_times = t_r * ((np.arange(n_scans) + slice_time_ref)+idx*n_scans)
            # Now use Nilearn to create the design matrix from the events files
            frame_time_allrun.append(frame_times)
        
        conc_gii_data_std=np.concatenate(gii_allrun, axis=1)
        concat_frame_times=np.concatenate(frame_time_allrun, axis=0)
        concat_events=pd.concat(events_allrun, axis=0)
        concat_confounds=pd.concat(confounds_allrun, axis=0)
        nonan_confounds=concat_confounds.dropna(axis=1, how='any')
        
        design_matrix = make_first_level_design_matrix(
            concat_frame_times,
            events=concat_events,
            hrf_model=hrf_model,  # convolve with SPM's canonical HRF function
            drift_model=None,  # we use fMRIPrep's cosine regressors
            add_regs=nonan_confounds,
        )


        # set the design matrix's NaN value to 0?
        
        # z-score the design matrix to standardize it
        design_matrix_std = stats.zscore(design_matrix, axis=0)
        # add constant in to standardized design matrix since you cannot z-score a constant
        design_matrix_std["constant"] = np.ones(len(design_matrix_std)).astype(int)
        
        ### Run the GLM
        # Y std or not?
        Y = np.transpose(conc_gii_data_std)
        X = np.asarray(design_matrix_std)
        labels, estimates = run_glm(Y, X, n_jobs=-1)

        ### Define the contrasts
        contrast_matrix = np.eye(design_matrix.shape[1])
        basic_contrasts = dict(
            [
                (column, contrast_matrix[i])
                for i, column in enumerate(design_matrix.columns)
            ]
        )
        print(basic_contrasts)
        '''
        contrasts_old = {
        "AllvsNull": (
            basic_contrasts["adult"] 
            + basic_contrasts["child"] 
            + basic_contrasts["body"] 
            + basic_contrasts["limb"] 
            + basic_contrasts["JP_word"] 
            + basic_contrasts["JP_FF"] 
            + basic_contrasts["JP_CB"] 
            + basic_contrasts["JP_CS"] 
            + basic_contrasts["JP_SC"] 
        ),
        "PERvsNull": (
            basic_contrasts["JP_CB"] 
            + basic_contrasts["JP_SC"] 
        ),
        "LEXvsNull": (
            basic_contrasts["JP_CS"] 
            + basic_contrasts["JP_FF"] 
        ),    
        "PERvsLEX": (
            basic_contrasts["JP_CB"] / 2
            + basic_contrasts["JP_SC"] / 2
            - basic_contrasts["JP_CS"] / 2
            - basic_contrasts["JP_FF"] / 2
        ),          
        "WordvsLEX": (
            basic_contrasts["JP_word"] 
            - basic_contrasts["JP_CS"] / 2
            - basic_contrasts["JP_FF"] / 2
        ),  
        "WordvsPER": (
            basic_contrasts["JP_word"] 
            - basic_contrasts["JP_CB"] / 2
            - basic_contrasts["JP_SC"] / 2
        ),   
        "WordvsLEXPER": (
            basic_contrasts["JP_word"] 
            - basic_contrasts["JP_CS"] / 4
            - basic_contrasts["JP_FF"] / 4
            - basic_contrasts["JP_CB"] / 4
            - basic_contrasts["JP_SC"] / 4
        ),     
        "WordvsAllnoWordnoLEX": (
            basic_contrasts["JP_word"] 
            - basic_contrasts["JP_CB"] / 6
            - basic_contrasts["JP_SC"] / 6
            - basic_contrasts["body"] / 6
            - basic_contrasts["limb"] / 6
            - basic_contrasts["adult"] / 6
            - basic_contrasts["child"] / 6
        ),
        
        "WordvsAllnoWord": (
            basic_contrasts["JP_word"] 
            - basic_contrasts["JP_CS"] / 8
            - basic_contrasts["JP_FF"] / 8                
            - basic_contrasts["JP_CB"] / 8
            - basic_contrasts["JP_SC"] / 8
            - basic_contrasts["body"] / 8
            - basic_contrasts["limb"] / 8
            - basic_contrasts["adult"] / 8
            - basic_contrasts["child"] / 8
        ),     
        "LEXvsAllnoWordnoLEX": (
            basic_contrasts["JP_CS"] / 2
            + basic_contrasts["JP_FF"] / 2
            - basic_contrasts["JP_CB"] / 6
            - basic_contrasts["JP_SC"] / 6
            - basic_contrasts["body"] / 6
            - basic_contrasts["limb"] / 6
            - basic_contrasts["adult"] / 6
            - basic_contrasts["child"] / 6
        ),        
        "SCvsCB": (
            basic_contrasts["JP_SC"] 
            - basic_contrasts["JP_CB"] 
            
        ),     
        "CSvsFF": (
            basic_contrasts["JP_CS"] 
            - basic_contrasts["JP_FF"] 
            
        ),     
        "FacesvsNull": (
            basic_contrasts["adult"] 
            + basic_contrasts["child"] 
        ),    
        "FacesvsLEX": (
            basic_contrasts["adult"] / 2
            + basic_contrasts["child"] / 2
            - basic_contrasts["JP_CS"] / 2
            - basic_contrasts["JP_FF"]  / 2
        ), 
        "FacesvsPER": (
            basic_contrasts["adult"] / 2
            + basic_contrasts["child"] / 2
            - basic_contrasts["JP_CB"] / 2
            - basic_contrasts["JP_SC"]  / 2
        ),    
        "FacesvsLEXPER": (
            basic_contrasts["adult"] / 2
            + basic_contrasts["child"] / 2
            - basic_contrasts["JP_CB"] / 4
            - basic_contrasts["JP_SC"]  / 4
            - basic_contrasts["JP_CS"] / 4
            - basic_contrasts["JP_FF"]  / 4                
        ),   
        "FacesvsAllnoFace": (
            basic_contrasts["adult"] / 2
            + basic_contrasts["child"] / 2
            - basic_contrasts["JP_CB"] / 7
            - basic_contrasts["JP_SC"]  / 7
            - basic_contrasts["JP_CS"] / 7
            - basic_contrasts["JP_FF"]  / 7  
            - basic_contrasts["body"] / 7
            - basic_contrasts["limb"] / 7 
            - basic_contrasts["JP_word"] / 7            
        ),  
        "AdultvsChild": (
            basic_contrasts["adult"] 
            - basic_contrasts["child"] 
        ),        
        
        "LimbsvsNull": (
            basic_contrasts["body"] 
            + basic_contrasts["limb"] 
        ),    
        "LimbsvsLEX": (
            basic_contrasts["body"] / 2
            + basic_contrasts["limb"] / 2
            - basic_contrasts["JP_CS"] / 2
            - basic_contrasts["JP_FF"]  / 2
        ), 
        "LimbsvsPER": (
            basic_contrasts["body"] / 2
            + basic_contrasts["limb"] / 2
            - basic_contrasts["JP_CB"] / 2
            - basic_contrasts["JP_SC"]  / 2
        ),    
        "LimbsvsLEXPER": (
            basic_contrasts["body"] / 2
            + basic_contrasts["limb"] / 2
            - basic_contrasts["JP_CB"] / 4
            - basic_contrasts["JP_SC"]  / 4
            - basic_contrasts["JP_CS"] / 4
            - basic_contrasts["JP_FF"]  / 4                
        ),   
        "LimbsvsAllnoLimbs": (
            basic_contrasts["body"] / 2
            + basic_contrasts["limb"] / 2
            - basic_contrasts["JP_CB"] / 7
            - basic_contrasts["JP_SC"]  / 7
            - basic_contrasts["JP_CS"] / 7
            - basic_contrasts["JP_FF"]  / 7  
            - basic_contrasts["adult"] / 7
            - basic_contrasts["child"] / 7 
            - basic_contrasts["JP_word"] / 7              
        ),  
        "BodyvsLimb": (
            basic_contrasts["body"] 
            - basic_contrasts["limb"] 
        )                                                                                                                                                        
        
    }
    '''
        # Extract contrasts
        contrasts_yaml = l1_glm_yaml['contrasts']
        contrast_groups=l1_glm_yaml['contrast_groups']
        contrasts = {}
        for contrast in contrasts_yaml:
            contrast_vector = np.zeros_like(next(iter(basic_contrasts.values())), dtype=np.float64)
            plus=contrast.split('vs')[0]
            minus=contrast.split('vs')[1]
            try:
                if minus == "Null":
                    for part in contrast_groups[plus]:
                        contrast_vector += basic_contrasts[part]
                    contrasts[contrast]=contrast_vector
                else:
                    con1=contrast_groups[plus]
                    factor1=len(con1)
                    con2=contrast_groups[minus]
                    factor2=len(con2)
                    for part in con1:
                        contrast_vector += basic_contrasts[part]/factor1
                    for part in con2:
                        contrast_vector -= basic_contrasts[part]/factor2        
                    contrasts[contrast]=contrast_vector
            except KeyError as e :
                logger.error(f"{contrast} contrast have key error, check your yaml")
                ### Compute the contrasts
            for index, (contrast_id, contrast_val) in enumerate(contrasts.items()):
                # Add a label to the output dictionary if not present
                if contrast_id not in contrast_objs:
                    contrast_objs[contrast_id] = []
                    
                # Define a name template for output statistical maps (stat-X is replaced later on)
                outname_base_run = f"sub-{subject}_ses-{session}_task-{task}_hemi-{hemi}_space-{space}_contrast-{contrast_id}_stat-X_statmap.func.gii"
                if use_smoothed:
                    outname_base_run = outname_base_run.replace(
                        "_statmap", f"_desc-smoothed{time_series_smooth_kernel}_statmap"
                    )
                outname_base_run = op.join(outdir, outname_base_run)  # Place in output directory

                # compute contrast-related statistics
                contrast = compute_contrast(
                    labels, estimates, contrast_val, contrast_type="t"
                )
                # add contrast to the output dictionary
                contrast_objs[contrast_id].append(contrast)

                # do the run-specific processing
                betas = contrast.effect_size()
                z_score = contrast.z_score()
                t_value = contrast.stat()
                p_value = contrast.p_value()
                variance = contrast.effect_variance()

                # Save the value maps as GIFTIs
                # Effect size
                outname = outname_base_run.replace("stat-X", "stat-effect")
                save_statmap_to_gifti(betas, outname)

                # z-score
                outname = outname_base_run.replace("stat-X", "stat-z")
                save_statmap_to_gifti(z_score, outname)

                # t-value
                outname = outname_base_run.replace("stat-X", "stat-t")
                save_statmap_to_gifti(t_value, outname)

                # p-value
                outname = outname_base_run.replace("stat-X", "stat-p")
                save_statmap_to_gifti(p_value, outname)

                # variance
                outname = outname_base_run.replace("stat-X", "stat-variance")
                save_statmap_to_gifti(variance, outname)
 
    return f"run_glm ingg for {subject} {session}"

def main():
    parser = argparse.ArgumentParser(description='Run l1_glm with input arguments through the function l1_glm')
    parser.add_argument('--subject', required=True, type=str, help='Subject for l1_glm')
    parser.add_argument('--session', required=True, type=str, help='Session for l1_glm')
    parser.add_argument('--lc_config', required=True, type=str, help='LC config yaml for l1_glm')
    parser.add_argument('--l1_glm_yaml', required=True, type=str, help='L1 GLM YAML file for l1_glm')
    
    args = parser.parse_args()
    lc_config=do.read_yaml(args.lc_config)
    l1_glm_yaml=do.read_yaml(args.l1_glm_yaml)
    # Call the function with the provided arguments
    run_l1_glm(args.subject, args.session, lc_config, l1_glm_yaml)

if __name__ == "__main__":
    main()
