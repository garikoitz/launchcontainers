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


def check_estimates_lines(folder_path):
    # Find all .out files in the folder
    out_files = [f for f in os.listdir(folder_path) if f.endswith('.out')]
    if not out_files:
        print('No .out files found in the folder.')
        return

    for out_file in out_files:
        out_file_path = os.path.join(folder_path, out_file)
        with open(out_file_path) as f:
            lines = f.readlines()
            count = sum(1 for line in lines if line.strip().startswith('Writing the estimates'))
            if count != 6:
                print(f"{out_file} has {count} lines starting with 'Writing the estimates' (expected 6).")


# Example usage:
folder = '/scratch/tlei/VOTCLOC/dipc_slurm_prfanalyze-vista_logs/march29'
check_estimates_lines(folder)
