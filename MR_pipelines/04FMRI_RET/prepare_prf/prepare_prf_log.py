# """
# MIT License
# Copyright (c) 2024-2025 Yongning Lei
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to permit persons to
# whom the Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.
# """
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
import pandas as pd
from scipy.io import loadmat


def prepare_prf(basedir, sub, ses, bids_folder_name, force):
    '''
    Description

    This function will first prepare the general folder structure under either
    BIDS dir you are specifying

    Then it will give instructions of where to get the source files,
    you need to find it and put them in place

    if there are targ_nifti_ana, then it will create things in both BIDS and targ_nifti

    '''
    if bids_folder_name in ['BIDS', 'nifti']:
        bids_dir = path.join(basedir, bids_folder_name)
        print(f'We will do prepare_prf on {bids_dir} for sub-{sub} ses-{ses}')
        sourcedata_dir = create_sourcedata_dir(bids_dir, sub, ses, force)
        print(f'#### vistadisplog folder created at {bids_dir}, \n \
        you need to copy the corresponding files there, and then run link_vistadisplog ')

    return sourcedata_dir


def create_sourcedata_dir(basedir, sub, ses, force):
    sourcedata_dir = path.join(basedir, 'sourcedata')
    stim_dir = path.join(sourcedata_dir, 'stimuli')
    vistadisp_dir = path.join(sourcedata_dir, 'vistadisplog')
    subses_dir = path.join(vistadisp_dir, f'sub-{sub}', f'ses-{ses}')
    srcdata_subses_dir = path.join(sourcedata_dir, f'sub-{sub}', f'ses-{ses}')

    if not path.exists(sourcedata_dir):
        print('The PRF sourcedata dir is not there, creating')
        os.makedirs(sourcedata_dir)

    if not path.exists(vistadisp_dir):
        print('The PRF sourcedata vistadisplog dir is not there, creating')
        os.makedirs(vistadisp_dir)

    if not path.exists(stim_dir):
        print('The PRF sourcedata stim dir is not there, creating')
        os.makedirs(stim_dir)

    if not path.exists(path.join(vistadisp_dir, f'sub-{sub}')):
        print('The PRF sourcedata sub dir is not there, creating')
        os.makedirs(path.join(vistadisp_dir, f'sub-{sub}'))

    if path.islink(subses_dir) and force:
        print(f'{subses_dir} exists, you choose to overwrite, overwritting')
        unlink(subses_dir)
        symlink(srcdata_subses_dir, subses_dir)
        print(f'symlink created for {srcdata_subses_dir} at {subses_dir}')
    else:
        symlink(srcdata_subses_dir, subses_dir)
        print(f'symlink created for  {srcdata_subses_dir} at {subses_dir}')

    return sourcedata_dir


def link_vistadisplog(sourcedata, sub, ses, force, task='ret'):
    '''
    '''
    print('Staring to create vistadisplog link')
    CB = 1
    FF = 1
    RW = 1
    fixRW = 1
    fixFF = 1
    fixRWblock = 1
    fixRWblock01 = 1
    fixRWblock02 = 1
    fixRWblock = 1
    matFiles = np.sort(
        glob(
            path.join(
                sourcedata, 'vistadisplog',
                f'sub-{sub}', f'ses-{ses}', '20*.mat',
            ),
        ),
    )
    if matFiles.size != 0 :
        print('Got the matfiles, going to start symlink')
    else:
        print(f'##### sub-{sub} ses-{ses} Not get the matfiles, please check path')
    for matFile in matFiles:

        stimName = loadmat(matFile, simplify_cells=True)['params']['loadMatrix']
        print(f'{stimName}')

        if 'CB_' in stimName:
            if 'tr-2' in stimName:
                linkName = path.join(
                    path.dirname(matFile),
                    f'sub-{sub}_ses-{ses}_task-retCB_run-0{CB}_params.mat',
                )
                CB += 1
        if 'FF_' in stimName:
            if 'tr-2' in stimName:
                linkName = path.join(
                    path.dirname(matFile),
                    f'sub-{sub}_ses-{ses}_task-retFF_run-0{FF}_params.mat',
                )
                FF += 1
        if 'RW_' in stimName:
            if 'tr-2' in stimName:
                linkName = path.join(
                    path.dirname(matFile),
                    f'sub-{sub}_ses-{ses}_task-retRW_run-0{RW}_params.mat',
                )
                RW += 1

        # for the wordcenter condition
        if 'fixRW_' in stimName:
            if 'tr-2' in stimName:
                linkName = path.join(
                    path.dirname(matFile),
                    f'sub-{sub}_ses-{ses}_task-retfixRW_run-0{CB}_params.mat',
                )
                fixRW += 1
        if 'fixFF_' in stimName:
            if 'tr-2' in stimName:
                linkName = path.join(
                    path.dirname(matFile),
                    f'sub-{sub}_ses-{ses}_task-retfixFF_run-0{FF}_params.mat',
                )
                fixFF += 1
        if 'fixRWblock01_' in stimName:
            if 'tr-2' in stimName:
                linkName = path.join(
                    path.dirname(
                        matFile,
                    ), f'sub-{sub}_ses-{ses}_task-retfixRWblock01_run-0{RW}_params.mat',
                )
                fixRWblock01 += 1
        if 'fixRWblock02_' in stimName:
            if 'tr-2' in stimName:
                linkName = path.join(
                    path.dirname(
                        matFile,
                    ), f'sub-{sub}_ses-{ses}_task-retfixRWblock02_run-0{RW}_params.mat',
                )
                fixRWblock02 += 1

        if 'fixRWblock_' in stimName:
            if 'tr-2' in stimName:
                linkName = path.join(
                    path.dirname(
                        matFile,
                    ), f'sub-{sub}_ses-{ses}_task-retfixRWblock_run-0{RW}_params.mat',
                )
                fixRWblock += 1
        if path.islink(linkName) and force:
            unlink(linkName)
            symlink(path.basename(matFile), linkName)
            print(f'symlink created for {path.basename(matFile)} at {linkName}')
        else:
            # print(f'src dir of the matfile is {path.basename(matFile)},
            # and the link name is {linkName} ')
            symlink(path.basename(matFile), linkName)
            print(f'symlink created for {path.basename(matFile)} with {linkName}')


def check_params_and_bids(layout, sub, ses):
    # need to check if params and bids task name match,
    # they might not match because I put fixRWblock as CB
    return


def main():
    # for bcbl /bcbl/home/public/Gari/VOTCLOC/main_exp
    # for dipc it is /scratch/tlei/VOTCLOC
    basedir = '/bcbl/home/public/Gari/VOTCLOC/main_exp'
    subseslist_fpath = path.join(basedir, 'code', 'subseslist_fmriprep.txt')
    subseslist = pd.read_csv(subseslist_fpath, sep=',', header=0, dtype='str')
    bids_folder_name = 'BIDS'
    force = True
    # first, need to set copied_mat to False, to create the vistadisplog foler, \
    # the vistadisplog folder will point to sourcedata/sub/ses
    # then set copied_mat to True, and then run link_vistadisplog
    copied_mat = True
    task = 'ret'
    sourcedata_dir = path.join(basedir, bids_folder_name , 'sourcedata')
    for idx, row in subseslist.iterrows():
        sub = row['sub']
        ses = row['ses']
        if not copied_mat:
            prepare_prf(basedir, sub, ses, bids_folder_name, force)
        else:
            link_vistadisplog(sourcedata_dir, sub, ses, force, task)


if __name__ == '__main__':
    main()
