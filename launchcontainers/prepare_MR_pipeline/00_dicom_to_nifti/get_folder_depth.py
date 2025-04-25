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

import os


def get_folder_depth(folder_path):
    max_depth = 0
    for root, dirs, files in os.walk(folder_path):
        # Calculate the depth of the current directory
        current_depth = root[len(folder_path):].count(os.sep)
        max_depth = max(max_depth, current_depth)
    return max_depth


# Example usage
folder_path = '/bcbl/home/public/Gari/VOTCLOC/main_exp/VOTCLOC_S03'  # Replace with your folder path
total_layers = get_folder_depth(folder_path)
print(f'Total layers in the folder: {total_layers}')
