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
from __future__ import annotations

import logging
import os
import subprocess as sp
log = logging.getLogger(__name__)


def sep_neuropythy(fullpath: str):
    """Extract the individual volumetric labels.

    Returns:
        [None]: [None]

    """
    join = os.path.join

    (head, tail) = os.path.split(fullpath)

    # if Aseg do not have left and right mask, generate them
    # aseg = join(head, "aparc+aseg.nii.gz")
    # if not os.path.isfile(join(head, "lh.AsegMask.nii.gz")):
    #     createHemiMaskFromAseg(aseg)

    # Detect the type of segmentation, Benson or Wang
    if 'benson' in tail:
        dic_roi = {
            1: 'V1',
            2: 'V2',
            3: 'V3',
            4: 'hV4',
            5: 'VO1',
            6: 'VO2',
            7: 'LO1',
            8: 'LO2',
            9: 'TO1',
            10: 'TO2',
            11: 'V3b',
            12: 'V3a',
        }
    elif 'wang' in tail:
        dic_roi = {
            1: 'wang_V1v',
            2: 'wang_V1d',
            3: 'wang_V2v',
            4: 'wang_V2d',
            5: 'wang_V3v',
            6: 'wang_V3d',
            7: 'wang_hV4',
            8: 'wang_VO1',
            9: 'wang_VO2',
            10: 'wang_PHC1',
            11: 'wang_PHC2',
            12: 'wang_MST',
            13: 'wang_hMT',
            14: 'wang_LO2',
            15: 'wang_LO1',
            16: 'wang_V3b',
            17: 'wang_V3a',
            18: 'wang_IPS0',
            19: 'wang_IPS1',
            20: 'wang_IPS2',
            21: 'wang_IPS3',
            22: 'wang_IPS4',
            23: 'wang_IPS5',
            24: 'wang_SPL1',
            25: 'wang_FEF',
        }
    else:
        log.info("The file '%s' does not contain benson or wang, returning", tail)
        return

    for index in dic_roi:
        dilstr = ['']
        diloption = ['']
        for i in range(len(dilstr)):
            roiname = join(head, 'ROIs', f'{dic_roi[index]}{dilstr[i]}.nii.gz')
            # extract benson varea
            cmd = f'mri_extract_label {diloption[i]} {fullpath} {index} {roiname}'
            log.info("Command for extracting labels:\n'%s'", cmd)
            a = sp.run(cmd, shell=True, check=False)
            if a == 1:
                if os.path.exists(roiname):
                    os.remove(roiname)
                continue
            # mask left and right hemisphere
            # extract the left
            head_tail = os.path.split(roiname)
            lhname = join(head_tail[0], 'Left-' + head_tail[1])
            rhname = join(head_tail[0], 'Right-' + head_tail[1])
            # extract the left
            cmd = (
                'mri_binarize '
                f"--mask {join(head, 'lh.AsegMask.nii.gz')} "
                ' --min 0.1 '
                f'--i {roiname} '
                f' --o {lhname}'
            )
            log.info("Command for left mri_binarize:\n'%s'", cmd)
            sp.run(cmd, shell=True, check=False)
            # extract the right
            cmd = (
                'mri_binarize '
                f"--mask {join(head, 'rh.AsegMask.nii.gz')} "
                ' --min 0.1 '
                f'--i {roiname} '
                f' --o {rhname}'
            )
            log.info("Command for right mri_binarize:\n'%s'", cmd)
            sp.run(cmd, shell=True, check=False)
            os.remove(roiname)
