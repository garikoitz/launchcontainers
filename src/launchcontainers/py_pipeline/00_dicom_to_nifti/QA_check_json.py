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
import os
import json

# Define the base directory
basedir = "/bcbl/home/public/Gari/VOTCLOC/fLoc_pilot"
bids_dir_name="no_dwi"
sub="02"
ses="day5BCBL"

ses_dir=os.path.join(basedir,bids_dir_name,f'sub-{sub}',f'ses-{ses}')

cat_list=['anat','fmap','dwi','func']

def get_coil_element(cat_dir):
    # Iterate through subfolders under the session
    for root, dirs, files in os.walk(cat_dir):
        session_info = []
        for file in files:
            if file.endswith(".json"):  # Process only JSON files
                file_path = os.path.join(root, file)
                # Read the JSON file
                with open(file_path, 'r') as json_file:
                    data = json.load(json_file)
                
                # Extract the required fields
                receive_coil_elements = data.get("ReceiveCoilActiveElements", "N/A")
                receive_coil_name = data.get("ReceiveCoilName", "N/A")
                
                # Get folder category and filename
                folder_category = os.path.basename(root)
                filename = os.path.splitext(file)[0]
                
                # Create a row for the session info
                session_info.append(
                    f"{folder_category}\t{filename}\t{receive_coil_elements}\t{receive_coil_name}"
                )
        
        # Write the session info to a text file in the session folder
        if session_info:
            output_file = os.path.join(cat_dir, "session_summary.txt")
            with open(output_file, 'w') as out_file:
                out_file.write("category\tfilename\treceiving coil elements\treceiving coil name\n")
                out_file.write("\n".join(session_info))

    print("Processing complete. Summary files created under each session.")