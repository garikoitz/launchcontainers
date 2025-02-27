#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
import numpy as np
import os.path as path
from os import rename
import os 
from bids import BIDSLayout
import bids
import json

basedir='/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS'
layout = BIDSLayout(basedir)

#subs = layout.get(return_type='id', target='subject')
subs= ['03']

for sub in subs:

    sess = layout.get(subject=sub, return_type='id', target='session')

    print(f'working on {sub}...')

    for ses in sess:
        print(f'working on {ses}')
        # load func and fmaps
        funcNiftis = layout.get(subject=sub, session=ses, extension='.nii.gz', suffix='magnitude',datatype='func')
        fmapNiftis = layout.get(subject=sub, session=ses, extension='.nii.gz', datatype='fmap')

        funcN = funcNiftis
        fmapN = fmapNiftis
        
        # add list to IntendedFor field in fmap json, add B0fieldIdentifier in fmap json
        for fmapNifti in fmapN:
            if not path.exists(fmapNifti.path.replace('.nii.gz', '_orig.json')):
                print('FMAP NO orig')
                f = fmapNifti.path.replace('.nii.gz', '.json')

                with open(f, 'r') as file:
                    j = json.load(file)

                if not 'IntendedFor' in j:
                    print(f'intendedfor not in {f}')
                     
                if not 'B0FieldIdentifier' in j:
                    print(f'B0FieldIdentifier not in {f}')
            else:
                print('FMPA Orig EXISTS!')
                f = fmapNifti.path.replace('.nii.gz', '.json')

                with open(f, 'r') as file:
                    j = json.load(file)

                if not 'IntendedFor' in j:
                    print(f'intendedfor not in {f}')
                     
                if not 'B0FieldIdentifier' in j:
                    print(f'B0FieldIdentifier not in {f}')
        # add B0source in bold.json
        for funcNifti in funcN:
            if not path.exists(funcNifti.path.replace('_bold.nii.gz', '_bold_orig.json')):
                print('func NOOO orig')
                f = funcNifti.path.replace('.nii.gz', '.json')

                with open(f, 'r') as file:
                    j = json.load(file)

                if not 'B0FieldSource' in j:
                    print(f'No B0FieldSource in func json {f}')
            else:
                print('Func orig EXISTS')
                f = funcNifti.path.replace('.nii.gz', '.json')
                with open(f, 'r') as file:
                    j = json.load(file)

                if not 'B0FieldSource' in j:
                    print(f'No B0FieldSource in func json {f}')