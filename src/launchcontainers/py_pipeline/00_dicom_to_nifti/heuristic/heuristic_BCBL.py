# -----------------------------------------------------------------------------
# Copyright (c) Yongning Lei 2024
# All rights reserved.
#
# This script is distributed under the Apache-2.0 license.
# You may use, distribute, and modify this code under the terms of the Apache-2.0 license.
# See the LICENSE file for details.
#
# THIS SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE, AND NONINFRINGEMENT.
#
# Author: Yongning Lei
# Email: yl4874@nyu.edu
# GitHub: https://github.com/yongninglei
# -----------------------------------------------------------------------------
import os


def create_key(template, outtype=('nii.gz',), annotation_classes=None):
    if template is None or not template:
        raise ValueError('Template must be a valid format string')
    return template, outtype, annotation_classes


def infotodict(seqinfo):
    """Heuristic evaluator for determining which runs belong where

    allowed template fields - follow python string module:

    item: index within category
    subject: participant id
    seqitem: run number during scanning
    subindex: sub index within group
    """
    # T1 MP2RAGE 
    t1_i1 = create_key('sub-{subject}/{session}/anat/sub-{subject}_{session}_run-{item:02d}_T1_inv1')
    t1_i2 = create_key('sub-{subject}/{session}/anat/sub-{subject}_{session}_run-{item:02d}_T1_inv2')
    t1_un = create_key('sub-{subject}/{session}/anat/sub-{subject}_{session}_run-{item:02d}_T1_uni')
    
    # T1 weighted MPRAGE
    t1_w = create_key('sub-{subject}/{session}/anat/sub-{subject}_{session}_run-{item:02d}_T1w_mprage')
    
    # fmap
    fmap_AP= create_key('sub-{subject}/{session}/fmap/sub-{subject}_{session}_dir-AP_run-{item:01d}_epi')
    fmap_PA= create_key('sub-{subject}/{session}/fmap/sub-{subject}_{session}_dir-PA_run-{item:01d}_epi')
    

    #func
    fLoc_sbref = create_key('sub-{subject}/{session}/func/sub-{subject}_{session}_task-fLoc_run-{item:02d}_sbref')
    fLoc_P     = create_key('sub-{subject}/{session}/func/sub-{subject}_{session}_task-fLoc_run-{item:02d}_phase')
    fLoc_M     = create_key('sub-{subject}/{session}/func/sub-{subject}_{session}_task-fLoc_run-{item:02d}_magnitude')  
    
    # dwi
    # nordic dwi
    dwi_floc1d5_rpe= create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-floc1d5isodir6_dir-PA_run-{item:02d}_magnitude')
    dwi_floc1d5= create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-floc1d5isodir104_dir-AP_run-{item:02d}_magnitude')
    dwi_floc1d5_rpe_pha= create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-floc1d5isodir6_dir-PA_run-{item:02d}_phase')
    dwi_floc1d5_pha= create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-floc1d5isodir104_dir-AP_run-{item:02d}_phase')    
    
    dwi_bcbl2_rpe= create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-bcbl2dir6_dir-PA_run-{item:02d}_magnitude')
    dwi_bcbl2= create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-bcbl2dir104_dir-AP_run-{item:02d}_magnitude')
    dwi_bcbl2_rpe_pha= create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-bcbl2dir6_dir-PA_run-{item:02d}_phase')
    dwi_bcbl2_pha= create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-bcbl2dir104_dir-AP_run-{item:02d}_phase')   

    dwi_tamagawa2_rpe= create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-tama2isodir6_dir-AP_run-{item:02d}_magnitude')
    dwi_tamagawa2= create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-tama2isodir104_dir-PA_run-{item:02d}_magnitude')
    dwi_tamagawa2_rpe_pha= create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-tama2isodir6_dir-AP_run-{item:02d}_phase')
    dwi_tamagawa2_pha= create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-tama2isodir104_dir-PA_run-{item:02d}_phase')    
    
    dwi_okazaki1d7_rpe= create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-okaz1d7isodir6_dir-AP_run-{item:02d}_magnitude')
    dwi_okazaki1d7= create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-okaz1d7isodir68_dir-PA_run-{item:02d}_magnitude')  
    dwi_okazaki1d7_rpe_pha= create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-okaz1d7isodir6_dir-AP_run-{item:02d}_phase')
    dwi_okazaki1d7_pha= create_key('sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-okaz1d7isodir68_dir-PA_run-{item:02d}_phase')

    
    info = {   
    t1_i1: [], t1_i2: [], t1_un: [], t1_w:[],
    fmap_AP: [], fmap_PA: [],
    fLoc_sbref:[], fLoc_P:[], fLoc_M:[],
    dwi_floc1d5_rpe:[], dwi_floc1d5:[],
    dwi_tamagawa2_rpe:[], dwi_tamagawa2:[],
    dwi_okazaki1d7_rpe:[], dwi_okazaki1d7:[],
    
    dwi_floc1d5_rpe_pha:[], dwi_floc1d5_pha:[],
    dwi_tamagawa2_rpe_pha:[], dwi_tamagawa2_pha:[],
    dwi_okazaki1d7_rpe_pha:[], dwi_okazaki1d7_pha:[],

    dwi_bcbl2_rpe:[], dwi_bcbl2:[], dwi_bcbl2_rpe_pha:[], dwi_bcbl2_pha:[]
    }
    last_run = len(seqinfo)

    for s in seqinfo:
        """
        The namedtuple `s` contains the following fields:

        * total_files_till_now
        * example_dcm_file
        * series_id
        * dcm_dir_name
        * unspecified2
        * unspecified3
        * dim1
        * dim2
        * dim3
        * dim4
        * TR
        * TE
        * protocol_name
        * is_motion_corrected
        * is_derived
        * patient_id
        * study_description
        * referring_physician_name
        * series_description
        * image_type
        """
        #T1
        if (s.dim1 == 256) and (s.dim2 == 240) and (s.dim3== 176) and ('mp2rage' in s.protocol_name):
            if ('_INV1' in s.series_description):
                info[t1_i1].append(s.series_id)
            elif ('_INV2' in s.series_description):
                info[t1_i2].append(s.series_id)
            elif ('_UNI' in s.series_description):
                info[t1_un].append(s.series_id)
        if (s.dim1 == 256) and (s.dim2 == 256) and (s.dim3== 176) and ('mprage' in s.protocol_name):
            info[t1_w].append(s.series_id)
        
        #fmap
        if ('TOPUP' in s.protocol_name.upper()): # and ('M' in s.image_type):
            if (s.dim1 == 92) and (s.dim3 == 80) and (s.series_files == 1): # and (s.TR==14.956):
                if ('AP' in s.protocol_name) :
                    info[fmap_AP].append(s.series_id)
                if ('PA' in s.protocol_name) :
                    info[fmap_PA].append(s.series_id)

        # TR nor working for the XA30 func scan of pRFs

        # functional SBref
        if (s.series_files == 1) and ('Pha' not in s.series_description):
            # pay attention to add a check for language in the s.protocol_name when in the scanner, otherwise the multiple language thing 
            # will cause trouble
            if ('fLoc' in s.protocol_name) or ('floc' in s.protocol_name):
                info[fLoc_sbref].append(s.series_id)

        if (s.dim1 == 92) and (s.dim3 == 80) and ((s.series_files ==160) or (s.series_files ==159)):
            if ('Pha' in s.series_description) :
                if ('fLoc' in s.protocol_name) or ('floc' in s.protocol_name) :
                    info[fLoc_P].append(s.series_id)    
            else:
                if ('fLoc' in s.protocol_name) or ('floc' in s.protocol_name) :
                    info[fLoc_M].append(s.series_id)
        
        # dwi
        # only take the mag
        if (('M' in s.image_type) or ('Pha' not in s.series_description)) and ('SBRef' not in s.series_description):
            if ('1p7' in s.series_description) or ('1p7mmiso' in s.series_description):
                if ('AP' in s.series_description) and (s.series_files==6):
                    info[dwi_okazaki1d7_rpe].append(s.series_id)
                if ('PA' in s.series_description) and (s.series_files==69):
                    info[dwi_okazaki1d7].append(s.series_id)                   
            if ('2iso' in s.series_description) or ('dMRI' in s.series_description and '1p7' not in s.series_description):
                if ('AP' in s.series_description) and (s.series_files==6):
                    info[dwi_tamagawa2_rpe].append(s.series_id)
                if ('PA' in s.series_description) and (s.series_files==105):
                    info[dwi_tamagawa2].append(s.series_id)          
            if ('1d5iso' in s.series_description):
                if ('PA' in s.series_description) and (s.series_files==6):
                    info[dwi_floc1d5_rpe].append(s.series_id)
                if ('AP' in s.series_description) and (s.series_files==105):
                    info[dwi_floc1d5].append(s.series_id)   
            if ('diff' in s.series_description):
                if ('PA' in s.series_description) and (s.series_files==6 or s.series_files ==7 ):
                    info[dwi_bcbl2_rpe].append(s.series_id)
                if ('AP' in s.series_description) and (s.series_files==105):
                    info[dwi_bcbl2].append(s.series_id)      

        if (('P' in s.image_type) or ('Pha' in s.series_description)) and ('SBRef' not in s.series_description):
            if ('1p7' in s.series_description) or ('1p7mmiso' in s.series_description):
                if ('AP' in s.series_description) and (s.series_files==6):
                    info[dwi_okazaki1d7_rpe_pha].append(s.series_id)
                if ('PA' in s.series_description) and (s.series_files==69):
                    info[dwi_okazaki1d7_pha].append(s.series_id)                   
            if ('2iso' in s.series_description) or ('dMRI' in s.series_description and '1p7' not in s.series_description):
                if ('AP' in s.series_description) and (s.series_files==6):
                    info[dwi_tamagawa2_rpe_pha].append(s.series_id)
                if ('PA' in s.series_description) and (s.series_files==105):
                    info[dwi_tamagawa2_pha].append(s.series_id)          
            if ('1d5iso' in s.series_description):
                if ('PA' in s.series_description) and (s.series_files==6):
                    info[dwi_floc1d5_rpe_pha].append(s.series_id)
                if ('AP' in s.series_description) and (s.series_files==105):
                    info[dwi_floc1d5_pha].append(s.series_id)    
            if ('diff' in s.series_description):
                if ('PA' in s.series_description) and (s.series_files==6 or s.series_files ==7 ):
                    info[dwi_bcbl2_rpe_pha].append(s.series_id)
                if ('AP' in s.series_description) and (s.series_files==105):
                    info[dwi_bcbl2_pha].append(s.series_id)                       
    return info
