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

import json
import os

# Define the base directory
basedir = '/bcbl/home/public/Gari/VOTCLOC/fLoc_pilot'
bids_dir_name = 'no_dwi'
sub = '02'
ses = 'day5BCBL'

ses_dir = os.path.join(basedir, bids_dir_name, f'sub-{sub}', f'ses-{ses}')

cat_list = ['anat', 'fmap', 'dwi', 'func']


def get_coil_element(cat_dir):
    # Iterate through subfolders under the session
    for root, dirs, files in os.walk(cat_dir):
        session_info = []
        for file in files:
            if file.endswith('.json'):  # Process only JSON files
                file_path = os.path.join(root, file)
                # Read the JSON file
                with open(file_path) as json_file:
                    data = json.load(json_file)

                # Extract the required fields
                receive_coil_elements = data.get('ReceiveCoilActiveElements', 'N/A')
                receive_coil_name = data.get('ReceiveCoilName', 'N/A')

                # Get folder category and filename
                folder_category = os.path.basename(root)
                filename = os.path.splitext(file)[0]

                # Create a row for the session info
                session_info.append(
                    f'{folder_category}\t{filename}\t{receive_coil_elements}\t{receive_coil_name}',
                )

        # Write the session info to a text file in the session folder
        if session_info:
            output_file = os.path.join(cat_dir, 'session_summary.txt')
            with open(output_file, 'w') as out_file:
                out_file.write(
                    'category\tfilename\treceiving coil elements\treceiving coil name\n',
                )
                out_file.write('\n'.join(session_info))

    print('Processing complete. Summary files created under each session.')
