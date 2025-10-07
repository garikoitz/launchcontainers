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

    # T2 weighted
    t2_w = create_key('sub-{subject}/{session}/anat/sub-{subject}_{session}_run-{item:02d}_T2w')
    # fmap
    fmap_AP = create_key(
        'sub-{subject}/{session}/fmap/sub-{subject}_{session}_acq-fMRI_dir-AP_run-{item:01d}_epi',
    )
    fmap_PA = create_key(
        'sub-{subject}/{session}/fmap/sub-{subject}_{session}_acq-fMRI_dir-PA_run-{item:01d}_epi',
    )

    # dwi
    # nordic dwi
    dwi_floc1d5_rpe = create_key(
        'sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-floc1d5isodir6_dir-PA_run-{item:02d}_magnitude',
    )
    dwi_floc1d5 = create_key(
        'sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-floc1d5isodir104_dir-AP_run-{item:02d}_magnitude',
    )
    dwi_floc1d5_rpe_pha = create_key(
        'sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-floc1d5isodir6_dir-PA_run-{item:02d}_phase',
    )
    dwi_floc1d5_pha = create_key(
        'sub-{subject}/{session}/dwi/sub-{subject}_{session}_acq-floc1d5isodir104_dir-AP_run-{item:02d}_phase',
    )

    info = {
        t2_w: [],
        fmap_AP: [], fmap_PA: [],
        dwi_floc1d5_rpe: [], dwi_floc1d5: [],
        dwi_floc1d5_rpe_pha: [], dwi_floc1d5_pha: [],

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
        # T2
        if (s.dim1 == 256) and (s.dim2 == 232) and (s.dim3 == 176) and ('MGH' in s.protocol_name):
            info[t2_w].append(s.series_id)
        # fmap
        # and ('M' in s.image_type):
        if ('TOPUP' in s.protocol_name.upper()) or ('fmap' in s.protocol_name):
            if (s.dim1 == 92) and (s.dim3 == 80) and (s.series_files == 1):  # and (s.TR==14.956):
                if ('AP' in s.protocol_name) :
                    info[fmap_AP].append(s.series_id)
                if ('PA' in s.protocol_name) :
                    info[fmap_PA].append(s.series_id)

        # dwi
        # only take the mag
        if (('M' in s.image_type) or ('Pha' not in s.series_description)) and ('SBRef' not in s.series_description):
            if ('dMRI' in s.series_description) or ('1d5iso' in s.series_description):
                if ('PA' in s.series_description) and (s.series_files == 6):
                    info[dwi_floc1d5_rpe].append(s.series_id)
                if ('AP' in s.series_description) and (s.series_files == 105):
                    info[dwi_floc1d5].append(s.series_id)

        if (('P' in s.image_type) or ('Pha' in s.series_description)) and ('SBRef' not in s.series_description):
            if ('dMRI' in s.series_description) or ('1d5iso' in s.series_description):
                if ('PA' in s.series_description) and (s.series_files == 6):
                    info[dwi_floc1d5_rpe_pha].append(s.series_id)
                if ('AP' in s.series_description) and (s.series_files == 105):
                    info[dwi_floc1d5_pha].append(s.series_id)
    return info
