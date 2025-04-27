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
from __future__ import annotations

import os
import subprocess

import pandas as pd
from bids import BIDSLayout

'''
Note: needs to be run after the fMRI is ready, the func thing
'''


def sync_bids_component(src_bids_folder, targ_bids_folder, force):
    """
    Use it once when there is  nothing in the targ bids folder
    """
    bids_folder_componment = [
        'dataset_description.json',
        'participants.json',
        'participants.tsv',
        'README',
    ]
    for item in bids_folder_componment:
        # Ensure destination directory exists
        os.makedirs(targ_bids_folder, exist_ok=True)
        src_item = os.path.join(src_bids_folder, item)
        targ_item = os.path.join(targ_bids_folder, item)
        if os.path.exists(os.path.join(targ_bids_folder, item)) and not force:
            print(
                f'Target BIDS components {targ_item} already exist in \
                    {targ_bids_folder}. Skipping copy.',
            )
        else:
            rsync_command = ['rsync', '-av', src_item, targ_item]
            subprocess.run(rsync_command, check=True)
            print(f'Copied {src_item} to {targ_item}')


def copy_fmap_folders(src_bids_folder, targ_bids_folder, force, sub=None, ses=None):
    # Initialize BIDS Layout
    layout = BIDSLayout(src_bids_folder)
    if (sub or ses):
        print(f'Copy fmap folders Working on sub-{sub}, ses-{ses}')

        # Get all fmap directories by finding fmap files and extracting unique directories
        fmap_path = [
            i.dirname for i in layout.get(
                subject=sub, session=ses, datatype='fmap', extension='json',
            )
        ]
        fmap_dirs = list(set(fmap_path))

        for fmap_dir in fmap_dirs:
            # Construct corresponding destination path in targ_bids_folder
            relative_path = os.path.relpath(fmap_dir, src_bids_folder)
            dest_dir = os.path.join(targ_bids_folder, relative_path)
            if os.path.exists(dest_dir) and not force:
                print(f'Target fmap directory {dest_dir} already exists. Skipping copy.')
                continue

            # Ensure destination directory exists
            os.makedirs(dest_dir, exist_ok=True)

            # Run rsync command to copy fmap folder
            rsync_command = [
                'rsync', '-av', '--exclude=*_orig.json',
                fmap_dir + '/', dest_dir + '/',
            ]
            subprocess.run(rsync_command, check=True)
            print(f'Copied {fmap_dir} to {dest_dir}')
    else:
        print(f'Copy fmap folders Working on whole {targ_bids_folder}')

        # Get all fmap directories by finding fmap files and extracting unique directories
        fmap_path = [
            i.dirname for i in layout.get(
                subject=sub, session=ses, datatype='fmap', extension='json',
            )
        ]
        fmap_dirs = list(set(fmap_path))

        for fmap_dir in fmap_dirs:
            # Construct corresponding destination path in targ_bids_folder
            relative_path = os.path.relpath(fmap_dir, src_bids_folder)
            dest_dir = os.path.join(targ_bids_folder, relative_path)
            if os.path.exists(dest_dir) and not force:
                print(f'Target fmap directory {dest_dir} already exists. Skipping copy.')
                continue
            # Ensure destination directory exists
            os.makedirs(dest_dir, exist_ok=True)

            # Run rsync command to copy fmap folder
            rsync_command = [
                'rsync', '-av', '--exclude=*_orig.json',
                fmap_dir + '/', dest_dir + '/',
            ]
            subprocess.run(rsync_command, check=True)
            print(f'Copied {fmap_dir} to {dest_dir}')


def copy_scan_tsv(src_bids_folder, targ_bids_folder, force, sub=None, ses=None):
    # Initialize BIDS Layout
    layout = BIDSLayout(src_bids_folder)
    if (sub or ses):
        print(f'copy_scan_tsv Working on sub-{sub}, ses-{ses}')
        scan_tsv_path = [
            i for i in layout.get(
                subject=sub, session=ses, suffix='scans', extension='tsv', return_type='filename',
            )
        ]

        for scan_tsv in scan_tsv_path:
            # Construct corresponding destination path in targ_bids_folder
            relative_path = os.path.relpath(scan_tsv, src_bids_folder)
            dest_dir = os.path.join(targ_bids_folder, relative_path)
            print(f'### dest dir is {dest_dir}')

            if os.path.exists(dest_dir) and not force:
                print(f'Target scan TSV {dest_dir} already exists. Skipping copy.')
                continue
            # Ensure destination directory exists
            os.makedirs(os.path.dirname(dest_dir), exist_ok=True)
            # Run rsync command to copy fmap folder
            rsync_command = ['rsync', '-av', scan_tsv, dest_dir]
            subprocess.run(rsync_command, check=True)
            print(f'Copied {scan_tsv} to {os.path.dirname(dest_dir)}')
    else:
        print(f'copy_scan_tsv Working on {targ_bids_folder}')
        # Get all scan.tsv
        scan_tsv_path = [
            i for i in layout.get(
                suffix='scans', extension='tsv', return_type='filename',
            )
        ]

        for scan_tsv in scan_tsv_path:
            # Construct corresponding destination path in targ_bids_folder
            relative_path = os.path.relpath(scan_tsv, src_bids_folder)
            dest_dir = os.path.join(targ_bids_folder, relative_path)
            print(f'### dest dir is {dest_dir}')
            if os.path.exists(dest_dir) and not force:
                print(f'Target scan TSV {dest_dir} already exists. Skipping copy.')
                continue
            # Ensure destination directory exists
            os.makedirs(os.path.dirname(dest_dir), exist_ok=True)
            # Run rsync command to copy fmap folder
            rsync_command = ['rsync', '-av', scan_tsv, dest_dir]
            subprocess.run(rsync_command, check=True)
            print(f'Copied {scan_tsv} to {os.path.dirname(dest_dir)}')


def main():
    src_bids_folder = '/bcbl/home/public/Gari/VOTCLOC/main_exp/raw_nifti'  # Update this path
    targ_bids_folder = '/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS'  # Update this path
    force = False
    code_dir = '/bcbl/home/public/Gari/VOTCLOC/main_exp/code/01_prepare_nifti'
    subseslist = pd.read_csv(
        os.path.join(
            code_dir, 'subseslist_fmap.txt',
        ), sep='\t', dtype='str',
    )
    # This step will only need once
    sync_bids_component(src_bids_folder, targ_bids_folder, force)

    for row in subseslist.itertuples(index=False):
        sub = row.sub
        ses = row.ses
        print(sub, ses)
        copy_fmap_folders(src_bids_folder, targ_bids_folder, force , sub, ses)
        copy_scan_tsv(src_bids_folder, targ_bids_folder, force, sub, ses)


if __name__ == '__main__':
    main()
