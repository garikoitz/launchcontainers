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

def get_folder_depth(folder_path):
    max_depth = 0
    for root, dirs, files in os.walk(folder_path):
        # Calculate the depth of the current directory
        current_depth = root[len(folder_path):].count(os.sep)
        max_depth = max(max_depth, current_depth)
    return max_depth

# Example usage
folder_path = "/bcbl/home/public/Gari/VOTCLOC/main_exp/VOTCLOC_S03"  # Replace with your folder path
total_layers = get_folder_depth(folder_path)
print(f"Total layers in the folder: {total_layers}")