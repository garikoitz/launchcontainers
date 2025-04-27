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
import os.path as op
from os import makedirs

import nibabel as nib
import numpy as np
from nilearn.surface import load_surf_data


logger = logging.getLogger('GENERAL')


def save_statmap_to_gifti(data, outname):
    """Save a statmap to a gifti file.
    data: nilearn contrast model output, e.g., contrast.effect_size()
    outname: output file name
    """
    gii_to_save = nib.gifti.gifti.GiftiImage()
    gii_to_save.add_gifti_data_array(
        nib.gifti.gifti.GiftiDataArray(data=data, datatype='NIFTI_TYPE_FLOAT32'),
    )
    nib.save(gii_to_save, outname)


def get_t_map(sub, ses, contrast, tmapdir):
    if not sub == '05':
        allmaps_dir = op.join(tmapdir, f'sub-{sub}', f'ses-{ses}')
    else:
        allmaps_dir = op.join(tmapdir, f'ses-{ses}')

    p_valmaps = [i for i in os.listdir(allmaps_dir) if (f'{contrast}_stat-t' in i)]
    wordvsper = p_valmaps[0]

    wp_tmap1 = load_surf_data(op.join(allmaps_dir, wordvsper))
    number_of_vertices = len(wp_tmap1)
    return number_of_vertices, wp_tmap1


# create probalistic map
# read maps for session 1, threshold it with t=10
# then you will have a binary map
space = 'fsnative'
# tmapdir="/bcbl/home/public/Gari/VOTCLOC/VSS/derivatives/l1_surface/analysis-okazaki_fwhm02"  #7Gari_fwhm2
# sess=['day1VA', 'day2VA', 'day1VB' , 'day2VB' ,'day3PF']  #'day3PF', 'day5BCBL', 'day6BCBL'
# sub='04' # it is 160008
sm = '02'

contrast = 'WordvsPER'
sess = ['day1VA', 'day2VA', 'day1VB' , 'day2VB' , 'day3PF', 'day5BCBL', 'day6BCBL']
sub = '05'  # it is 160008
tmapdir = '/bcbl/home/public/Gari/VOTCLOC/VSS/derivatives/l1_surface/7Gari_fwhm2'
# read and average all

#
number_of_vertices, _ = get_t_map(sub, 'day1VA', contrast, tmapdir)
t_map = np.zeros((number_of_vertices,), dtype=float)

# add the 7 maps together, you will have a heatmap
for ses in sess:
    _, t_maps = get_t_map(sub, ses, contrast, tmapdir)

    t_map += t_maps

avg = t_map / len(sess)

# save the file for visualization
out_dir = f'/bcbl/home/public/Gari/VOTCLOC/VSS/derivatives/probalistic_map/sub-{sub}'
if not os.path.isdir(out_dir):
    makedirs(out_dir)
outname = f'avgAllses_{contrast}_{space}_sm{sm}.gii'
save_statmap_to_gifti(avg, op.join(out_dir, outname))
