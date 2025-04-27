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

import json
import os

import pandas as pd
from bids import BIDSLayout
from heudiconv import bids as hb


def remove_intended_for(json_path: str, force):
    """
    Removes the 'IntendedFor' field from a BIDS JSON file if it exists.

    Parameters:
    -----------
    json_path : str
        Path to the JSON file.

    Returns:
    --------
    None
        The function modifies the file in place, saving it without 'IntendedFor'.
    """
    try:
        # Load the JSON file
        with open(json_path) as f:
            data = json.load(f)

        # Check if 'IntendedFor' exists and remove it
        if 'IntendedFor' in data and force:
            del data['IntendedFor']
            print(f"Removed 'IntendedFor' from {json_path}")

            # Save the modified JSON file
            with open(json_path, 'w') as f:
                json.dump(data, f, indent=4)
        else:
            print(f"'IntendedFor' not found in {json_path}")

    except FileNotFoundError:
        print(f'Error: File not found - {json_path}')
    except json.JSONDecodeError:
        print(f'Error: Invalid JSON format - {json_path}')


matching_parameters = [
    'Shims',
    'ImagingVolume',
    'ModalityAcquisitionLabel',
    'CustomAcquisitionLabel',
    'PlainAcquisitionLabel',
    'Force',
]

criteria = ['First', 'Closest']


def main():
    basedir = '/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS'
    code_dir = '/bcbl/home/public/Gari/VOTCLOC/main_exp/code/01_prepare_nifti'
    subseslist = pd.read_csv(
        os.path.join(
            code_dir, 'subseslist_fmap.txt',
        ), sep='\t', dtype='str',
    )
    # force = True
    layout = BIDSLayout(basedir)
    for row in subseslist.itertuples(index=False):
        sub = row.sub
        ses = row.ses
        subsesdir = layout.get(subject=sub, session=ses, suffix='scans')[0].dirname
        print(f'############# populate intendedfor for sub {sub} ses {ses} ')
        hb.populate_intended_for(subsesdir, 'Shims' , 'First')


if __name__ == '__main__':
    main()
