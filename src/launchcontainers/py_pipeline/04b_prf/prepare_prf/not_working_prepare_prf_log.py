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
'''
code to prepare prf:
1. create sourcedata folder under /base/BIDS
    1.a get stimulus .mat and put it under stimulus folder
    1.b generate sub-xx/ses-xx flder under sourcedata/vistadisplog
2. You need to manually copy things to the sub-ses folder we created
'''
from __future__ import annotations

import os
from glob import glob
from os import path
from os import symlink
from os import unlink

import numpy as np
from dask.distributed import Client
from dask.distributed import LocalCluster
from scipy.io import loadmat


def create_sourcedata_dir(bids_dir, sub, ses):
    sourcedata_dir = path.join(bids_dir, 'sourcedata')
    stim_dir = path.join(sourcedata_dir, 'stimuli')
    vistadisp_dir = path.join(sourcedata_dir, 'vistadisplog')
    subses_dir = path.join(vistadisp_dir, f'sub-{sub}', f'ses-{ses}')

    # IF there is no sourcedata_dir, create one
    if not path.exists(sourcedata_dir):
        print('The PRF sourcedata dir is not there, creating')
        os.makedirs(sourcedata_dir)

    # if there is no vistadisp dir, create one
    if not path.exists(vistadisp_dir):
        print('The PRF sourcedata vistadisplog dir is not there, creating')
        os.makedirs(vistadisp_dir)

    if not path.exists(stim_dir):
        print('The PRF sourcedata stim dir is not there, creating')
        os.makedirs(stim_dir)

    if not path.exists(subses_dir):
        print('The PRF sourcedata subses dir is not there, creating')
        os.makedirs(subses_dir)

    return sourcedata_dir


def link_vistadisplog(bids_dir, matFile, force):
    '''
    '''
    print('Staring to create vistadisplog link')
    CB = 1
    FF = 1
    RW = 1
    fixRW = 1
    fixFF = 1
    fixRWblock01 = 1
    fixRWblock02 = 1

    matinfo = loadmat(matFile, simplify_cells=True)['params']
    stimName = matinfo['loadMatrix']
    print(f'{stimName}')
    try:
        sub = matinfo['PatientName'].split('s')[1].split('-')[-1].split('_')[0]
        ses = matinfo['PatientName'].split('s')[-1].split('-')[-1].split('_')[0]

        if 'CB_' in stimName:
            if 'tr-2' in stimName:
                linkName = path.join(
                    path.dirname(
                        matFile,
                    ), f'sub-{sub}', f'ses-{ses}', f'sub-{sub}_ses-{ses}_task-retCB_run-0{CB}_params.mat',
                )
                CB += 1
        if 'FF_' in stimName:
            if 'tr-2' in stimName:
                linkName = path.join(
                    path.dirname(
                        matFile,
                    ), f'sub-{sub}', f'ses-{ses}', f'sub-{sub}_ses-{ses}_task-retFF_run-0{FF}_params.mat',
                )
                FF += 1
        if 'RW_' in stimName:
            if 'tr-2' in stimName:
                linkName = path.join(
                    path.dirname(
                        matFile,
                    ), f'sub-{sub}', f'ses-{ses}', f'sub-{sub}_ses-{ses}_task-retRW_run-0{RW}_params.mat',
                )
                RW += 1
        # for the wordcenter condition
        if 'fixRW_' in stimName:
            if 'tr-2' in stimName:
                linkName = path.join(
                    path.dirname(
                        matFile,
                    ), f'sub-{sub}', f'sub-{sub}', f'ses-{ses}', f'sub-{sub}_ses-{ses}_task-retfixRW_run-0{CB}_params.mat',
                )
                fixRW += 1
        if 'fixFF_' in stimName:
            if 'tr-2' in stimName:
                linkName = path.join(
                    path.dirname(
                        matFile,
                    ), f'sub-{sub}', f'sub-{sub}', f'ses-{ses}', f'sub-{sub}_ses-{ses}_task-retfixFF_run-0{FF}_params.mat',
                )
                fixFF += 1
        if 'fixRWblock01_' in stimName:
            if 'tr-2' in stimName:
                linkName = path.join(
                    path.dirname(
                        matFile,
                    ), f'sub-{sub}', f'sub-{sub}', f'ses-{ses}', f'sub-{sub}_ses-{ses}_task-retfixRWblock01_run-0{RW}_params.mat',
                )
                fixRWblock01 += 1
        if 'fixRWblock02_' in stimName:
            if 'tr-2' in stimName:
                linkName = path.join(
                    path.dirname(
                        matFile,
                    ), f'sub-{sub}', f'sub-{sub}', f'ses-{ses}', f'sub-{sub}_ses-{ses}_task-retfixRWblock02_run-0{RW}_params.mat',
                )
                fixRWblock02 += 1

        create_sourcedata_dir(bids_dir, sub, ses)

        if path.islink(linkName) and force:
            unlink(linkName)
            symlink(matFile, linkName)
            print(f'symlink created for {path.basename(matFile)} at {linkName}')
        else:
            symlink(matFile, linkName)
            print(f'symlink created for {path.basename(matFile)} with {linkName}')
    except Exception as e:
        print(f'For matfile {matFile}, it is not working because {e}')


def main():
    basedir = '/bcbl/home/public/Gari/VOTCLOC/main_exp'
    bids_dir_name = 'BIDS'
    bids_dir = path.join(basedir, bids_dir_name)
    force = True
    # copied_mat=True
    sourcedata_dir = path.join(basedir, bids_dir_name , 'sourcedata')
    matFiles = np.sort(glob(path.join(sourcedata_dir, 'vistadisplog', '20*.mat')))
    if matFiles.size != 0 :
        print('Got the matfiles, going to start symlink')
    else:
        print('Not get the matfiles, please check path')

    cluster = LocalCluster(n_workers=15, threads_per_worker=2)  # Adjust based on system
    client = Client(cluster)

    futures = [client.submit(link_vistadisplog, bids_dir, matFile, force) for matFile in matFiles]
    # Collect results
    results = client.gather(futures)
    print(results)

    # Shutdown cluster after execution
    client.close()
    cluster.close()


if __name__ == '__main__':
    main()
