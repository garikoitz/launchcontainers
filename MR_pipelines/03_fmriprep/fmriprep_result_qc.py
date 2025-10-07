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

import argparse
import csv
import logging
import os
import os.path as op
import time
from argparse import RawDescriptionHelpFormatter
from os import makedirs

import matplotlib.pyplot as plt
import nibabel as nib
import numpy as np
import pandas as pd
import yaml
from bids import BIDSLayout
from nilearn.glm.contrasts import compute_contrast
from nilearn.glm.first_level import first_level_from_bids
from nilearn.glm.first_level import make_first_level_design_matrix
from nilearn.glm.first_level.first_level import run_glm
from nilearn.plotting import plot_design_matrix
from nilearn.surface import load_surf_data
from scipy import stats

logger = logging.getLogger('FMRIPREP')


### excecution
if __name__ == "__main__":
    # path to your fmriprep folder
    base_dir = "/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS/derivatives/fmriprep-25.1.4_t2_pial_dummyscan_5"  
    fmriprep_layout=BIDSLayout(base_dir, validate=False)
    subs=fmriprep_layout.get_subject()
    for sub in subs:
        sess=fmriprep_layout.get_session(subject=sub)
        for ses in sess:
            # get runs

            # for each run 
            # get:
            gii_func=fmriprep_layout.get(
                subject=sub,
                session=ses,
                datatype='func',task='fLoc',suffix='bold',space='fsnative',extension='func.gii',
                return_type='list')
            # get the confounds.tsv

            # get the events.tsv
            # if all exist, then we are good, 

            # if one of it doesn't exist, save the error to a df

            # save the df
            