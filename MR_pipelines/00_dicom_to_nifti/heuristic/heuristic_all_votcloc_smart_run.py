# """
# MIT License
# Copyright (c) 2024-2025 Yongning Lei
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or substantial
# portions of the Software.
# """
from __future__ import annotations
import re
from collections import defaultdict


def create_key(template, outtype=('nii.gz',), annotation_classes=None):
    if template is None or not template:
        raise ValueError('Template must be a valid format string')
    return template, outtype, annotation_classes


def extract_run_number(protocol_name):
    """
    Extract run number from protocol name.
    
    Examples:
        'floc_run01_TigerEdit' -> 1
        'prf_word_run02_TigerEdit' -> 2
        'floc_run10_TigerEdit' -> 10
    
    Returns:
        int or None: Run number if found, None otherwise
    """
    match = re.search(r'run(\d+)', protocol_name, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def infotodict(seqinfo):
    """Heuristic evaluator for determining which runs belong where

    allowed template fields - follow python string module:

    item: index within category
    subject: participant id
    seqitem: run number during scanning
    subindex: sub index within group
    """
    # T1 MP2RAGE
    t1_i1 = create_key(
        'sub-{subject}/{session}/anat/sub-{subject}_{session}_run-{item:02d}_T1_inv1',
    )
    t1_i2 = create_key(
        'sub-{subject}/{session}/anat/sub-{subject}_{session}_run-{item:02d}_T1_inv2',
    )
    t1_un = create_key(
        'sub-{subject}/{session}/anat/sub-{subject}_{session}_run-{item:02d}_T1_uni',
    )

    # T1 weighted MPRAGE
    t1_w = create_key('sub-{subject}/{session}/anat/sub-{subject}_{session}_run-{item:02d}_T1w')
    # T2 weighted
    t2_w = create_key('sub-{subject}/{session}/anat/sub-{subject}_{session}_run-{item:02d}_T2w')
    # fmap
    fmap_AP = create_key(
        'sub-{subject}/{session}/fmap/sub-{subject}_{session}_acq-fMRI_dir-AP_run-{item:01d}_epi',
    )
    fmap_PA = create_key(
        'sub-{subject}/{session}/fmap/sub-{subject}_{session}_acq-fMRI_dir-PA_run-{item:01d}_epi',
    )

    # func
    fLoc_sbref = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-fLoc_run-{item:02d}_sbref',
    )
    fLoc_P = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-fLoc_run-{item:02d}_phase',
    )
    fLoc_M = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-fLoc_run-{item:02d}_magnitude',
    )

    ret_RW_sbref = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-retRW_run-{item:02d}_sbref',
    )
    ret_RW_P = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-retRW_run-{item:02d}_phase',
    )
    ret_RW_M = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-retRW_run-{item:02d}_magnitude',
    )

    ret_FF_sbref = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-retFF_run-{item:02d}_sbref',
    )
    ret_FF_P = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-retFF_run-{item:02d}_phase',
    )
    ret_FF_M = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-retFF_run-{item:02d}_magnitude',
    )

    ret_CB_sbref = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-retCB_run-{item:02d}_sbref',
    )
    ret_CB_P = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-retCB_run-{item:02d}_phase',
    )
    ret_CB_M = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-retCB_run-{item:02d}_magnitude',
    )

    # dwi
    dwi_votcloc_rpe = create_key(
        'sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-votcloc1d5_dir-PA_run-{item:02d}_magnitude',
    )
    dwi_votcloc = create_key(
        'sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-votcloc1d5_dir-AP_run-{item:02d}_magnitude',
    )
    dwi_votcloc_rpe_pha = create_key(
        'sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-votcloc1d5_dir-PA_run-{item:02d}_phase',
    )
    dwi_votcloc_pha = create_key(
        'sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-votcloc1d5_dir-AP_run-{item:02d}_phase',
    )

    info = {
        t1_i1: [], t1_i2: [], t1_un: [], t1_w: [], t2_w: [],
        fmap_AP: [], fmap_PA: [],
        fLoc_sbref: [], fLoc_P: [], fLoc_M: [],
        ret_RW_sbref: [], ret_RW_P: [], ret_RW_M: [],
        ret_FF_sbref: [], ret_FF_P: [], ret_FF_M: [],
        ret_CB_sbref: [], ret_CB_P: [], ret_CB_M: [],
        dwi_votcloc_rpe: [], dwi_votcloc: [],
        dwi_votcloc_rpe_pha: [], dwi_votcloc_pha: [],
    }
    
    # Temporary storage to group functional scans by run number and task
    func_scans = {
        'fLoc': defaultdict(lambda: {'sbref_mag': [], 'sbref_pha': [], 'main_mag': [], 'main_pha': []}),
        'retRW': defaultdict(lambda: {'sbref_mag': [], 'sbref_pha': [], 'main_mag': [], 'main_pha': []}),
        'retFF': defaultdict(lambda: {'sbref_mag': [], 'sbref_pha': [], 'main_mag': [], 'main_pha': []}),
        'retCB': defaultdict(lambda: {'sbref_mag': [], 'sbref_pha': [], 'main_mag': [], 'main_pha': []}),
    }
    
    last_run = len(seqinfo)

    # First pass: collect and categorize all functional scans
    for s in seqinfo:
        run_num = extract_run_number(s.protocol_name)
        
        # Determine task type
        task = None
        if ('fLoc' in s.protocol_name or 'floc' in s.protocol_name):
            task = 'fLoc'
        elif (('RW' in s.protocol_name) or ('word' in s.protocol_name)) and ('block' not in s.protocol_name):
            task = 'retRW'
        elif ('FF' in s.protocol_name):
            task = 'retFF'
        elif (('CB' in s.protocol_name) or ('fixRWblock' in s.protocol_name)):
            task = 'retCB'
        
        # Categorize functional scans by task and run
        if task and run_num is not None and (s.dim1 == 92) and (s.dim3 == 80):
            is_sbref = s.series_files == 1
            is_main = (s.series_files == 160) or (s.series_files == 159) or (s.series_files == 156) or (s.series_files == 155)
            is_phase = 'Pha' in s.series_description
            
            if is_sbref:
                if is_phase:
                    func_scans[task][run_num]['sbref_pha'].append(s)
                else:
                    func_scans[task][run_num]['sbref_mag'].append(s)
            elif is_main:
                if is_phase:
                    func_scans[task][run_num]['main_pha'].append(s)
                else:
                    func_scans[task][run_num]['main_mag'].append(s)
    
    # Second pass: match SBRefs with main scans and populate info dict
    # fLoc
    for run_num, scans in func_scans['fLoc'].items():
        if scans['main_mag']:
            # Take the latest main scan (highest series_id = most recent)
            main_mag = max(scans['main_mag'], key=lambda x: int(x.series_id))
            info[fLoc_M].append({'item': run_num, 'series_id': main_mag.series_id})
            
            # Find closest SBRef by series_id
            if scans['sbref_mag']:
                closest_sbref = min(
                    scans['sbref_mag'],
                    key=lambda x: abs(int(x.series_id) - int(main_mag.series_id))
                )
                info[fLoc_sbref].append({'item': run_num, 'series_id': closest_sbref.series_id})
        
        if scans['main_pha']:
            main_pha = max(scans['main_pha'], key=lambda x: int(x.series_id))
            info[fLoc_P].append({'item': run_num, 'series_id': main_pha.series_id})
    
    # retRW
    for run_num, scans in func_scans['retRW'].items():
        if scans['main_mag']:
            main_mag = max(scans['main_mag'], key=lambda x: int(x.series_id))
            info[ret_RW_M].append({'item': run_num, 'series_id': main_mag.series_id})
            
            if scans['sbref_mag']:
                closest_sbref = min(
                    scans['sbref_mag'],
                    key=lambda x: abs(int(x.series_id) - int(main_mag.series_id))
                )
                info[ret_RW_sbref].append({'item': run_num, 'series_id': closest_sbref.series_id})
        
        if scans['main_pha']:
            main_pha = max(scans['main_pha'], key=lambda x: int(x.series_id))
            info[ret_RW_P].append({'item': run_num, 'series_id': main_pha.series_id})
    
    # retFF
    for run_num, scans in func_scans['retFF'].items():
        if scans['main_mag']:
            main_mag = max(scans['main_mag'], key=lambda x: int(x.series_id))
            info[ret_FF_M].append({'item': run_num, 'series_id': main_mag.series_id})
            
            if scans['sbref_mag']:
                closest_sbref = min(
                    scans['sbref_mag'],
                    key=lambda x: abs(int(x.series_id) - int(main_mag.series_id))
                )
                info[ret_FF_sbref].append({'item': run_num, 'series_id': closest_sbref.series_id})
        
        if scans['main_pha']:
            main_pha = max(scans['main_pha'], key=lambda x: int(x.series_id))
            info[ret_FF_P].append({'item': run_num, 'series_id': main_pha.series_id})
    
    # retCB
    for run_num, scans in func_scans['retCB'].items():
        if scans['main_mag']:
            main_mag = max(scans['main_mag'], key=lambda x: int(x.series_id))
            info[ret_CB_M].append({'item': run_num, 'series_id': main_mag.series_id})
            
            if scans['sbref_mag']:
                closest_sbref = min(
                    scans['sbref_mag'],
                    key=lambda x: abs(int(x.series_id) - int(main_mag.series_id))
                )
                info[ret_CB_sbref].append({'item': run_num, 'series_id': closest_sbref.series_id})
        
        if scans['main_pha']:
            main_pha = max(scans['main_pha'], key=lambda x: int(x.series_id))
            info[ret_CB_P].append({'item': run_num, 'series_id': main_pha.series_id})
    
    # Now process non-functional scans (T1, T2, fmap, DWI) - these use normal iteration
    for s in seqinfo:
        # T1
        if (s.dim1 == 256) and (s.dim2 == 240) and (s.dim3 == 176) and ('mp2rage' in s.protocol_name):
            if ('_INV1' in s.series_description):
                info[t1_i1].append(s.series_id)
            elif ('_INV2' in s.series_description):
                info[t1_i2].append(s.series_id)
            elif ('_UNI' in s.series_description):
                info[t1_un].append(s.series_id)
        if (s.dim1 == 256) and (s.dim2 == 256) and (s.dim3 == 176) and ('mprage' in s.protocol_name):
            info[t1_w].append(s.series_id)
        if (s.dim1 == 256) and (s.dim2 == 232) and (s.dim3 == 176) and ('MGH' in s.protocol_name):
            info[t2_w].append(s.series_id)
        
        # fmap
        if ('TOPUP' in s.protocol_name.upper()) or ('fmap' in s.protocol_name):
            if (s.dim1 == 92) and (s.dim3 == 80) and (s.series_files == 1):
                if ('AP' in s.protocol_name):
                    info[fmap_AP].append(s.series_id)
                if ('PA' in s.protocol_name):
                    info[fmap_PA].append(s.series_id)
        
        # dwi
        if (('M' in s.image_type) or ('Pha' not in s.series_description)) and ('SBRef' not in s.series_description):
            if ('dMRI' in s.series_description) or ('1d5iso' in s.series_description):
                if ('PA' in s.series_description) and (s.series_files == 6):
                    info[dwi_votcloc_rpe].append(s.series_id)
                if ('AP' in s.series_description) and (s.series_files == 105):
                    info[dwi_votcloc].append(s.series_id)

        if (('P' in s.image_type) or ('Pha' in s.series_description)) and ('SBRef' not in s.series_description):
            if ('dMRI' in s.series_description) or ('1d5iso' in s.series_description):
                if ('PA' in s.series_description) and (s.series_files == 6):
                    info[dwi_votcloc_rpe_pha].append(s.series_id)
                if ('AP' in s.series_description) and (s.series_files == 105):
                    info[dwi_votcloc_pha].append(s.series_id)
    
    return info
