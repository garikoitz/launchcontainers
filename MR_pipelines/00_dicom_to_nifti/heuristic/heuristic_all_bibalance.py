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
    # T1 weighted MPRAGE
    t1_w = create_key('sub-{subject}/{session}/anat/sub-{subject}_{session}_run-{item:02d}_T1w')
    # fmap
    fmap_AP = create_key(
        'sub-{subject}/{session}/fmap/sub-{subject}_{session}_acq-fMRI_dir-AP_run-{item:01d}_epi',
    )
    fmap_PA = create_key(
        'sub-{subject}/{session}/fmap/sub-{subject}_{session}_acq-fMRI_dir-PA_run-{item:01d}_epi',
    )
    # func
    fLoc_4min_sbref = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-fLoc4min_run-{item:02d}_sbref',
    )
    fLoc_4min_P = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-fLoc4min_run-{item:02d}_phase',
    )
    fLoc_4min_M = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-fLoc4min_run-{item:02d}_magnitude',
    )
    fLoc_2min_sbref = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-fLoc2min_run-{item:02d}_sbref',
    )
    fLoc_2min_P = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-fLoc2min_run-{item:02d}_phase',
    )
    fLoc_2min_M = create_key(
        'sub-{subject}/{session}/func/sub-{subject}_{session}_task-fLoc2min_run-{item:02d}_magnitude',
    )
    # dwi
    dwi_cmrr_rpe = create_key(
        'sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-cmrr_dir-PA_run-{item:02d}_dwi',
    )
    dwi_cmrr = create_key(
        'sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-cmrr_dir-AP_run-{item:02d}_dwi',
    )
    info = {
        t1_w: [],
        fmap_AP: [], fmap_PA: [],
        fLoc_4min_sbref: [], fLoc_4min_P: [], fLoc_4min_M: [],
        fLoc_2min_sbref: [], fLoc_2min_P: [], fLoc_2min_M: [],
        dwi_cmrr_rpe: [], dwi_cmrr: [],

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
        # T1
        if (s.dim1 == 224) and (s.dim2 == 224) and (s.dim3 == 160) and ('mprage' in s.protocol_name):
            info[t1_w].append(s.series_id)
        # fmap
        # and ('M' in s.image_type):
        if ('TOPUP' in s.protocol_name.upper()) or ('fmap' in s.protocol_name):
            if (s.dim1 == 92) and (s.dim3 == 80) and (s.series_files == 1):  # and (s.TR==14.956):
                if ('AP' in s.protocol_name) :
                    info[fmap_AP].append(s.series_id)
                if ('PA' in s.protocol_name) :
                    info[fmap_PA].append(s.series_id)

        # TR not working for the XA30 func scan of pRFs
        # functional SBref
        if (s.series_files == 1) and ('Pha' not in s.series_description):
            # pay attention to add a check for language in the s.protocol_name when in the scanner, otherwise the multiple language thing
            # will cause trouble
            if ('word' in s.protocol_name) and ('4min' in s.protocol_name):
                info[fLoc_4min_sbref].append(s.series_id)
            if ('word' in s.protocol_name) and ('2min' in s.protocol_name):
                info[fLoc_2min_sbref].append(s.series_id)

        if (s.dim1 == 92) and (s.dim3 == 80):
            if ((s.series_files == 121)) :
                if ('Pha' in s.series_description) :
                    info[fLoc_4min_P].append(s.series_id)
                else:
                    info[fLoc_4min_M].append(s.series_id)
            if ((s.series_files == 121) or (s.series_files == 61)) and (('fLoc2min' in s.protocol_name) or ('floc2min' in s.protocol_name)) :
                if ('Pha' in s.series_description) :
                    info[fLoc_2min_P].append(s.series_id)
                else:
                    info[fLoc_2min_M].append(s.series_id)
                    info[fLoc_P].append(s.series_id)
                else:
                    info[fLoc_M].append(s.series_id)
        # dwi
        # only take the mag
        if (('DIFFUSION' in s.image_type) and ('SBRef' not in s.series_description)):
            if ('diff' in s.series_description):
                if ('PA' in s.series_description) and (s.series_files == 6):
                    info[dwi_cmrr_rpe].append(s.series_id)
                if ('AP' in s.series_description) and (s.series_files == 105):
                    info[dwi_cmrr].append(s.series_id)
    return info
