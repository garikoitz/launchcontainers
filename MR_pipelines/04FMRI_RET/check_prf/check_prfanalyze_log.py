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

def check_prfprepare_logs(folder_path):
    # Find all .out files in the folder
    out_files = [f for f in os.listdir(folder_path) if f.endswith('.out') or f.endswith('.o')]
    if not out_files:
        print('No .out files found in the folder.')
        return

    for out_file in out_files:
        out_file_path = os.path.join(folder_path, out_file)
        with open(out_file_path) as f:
            lines = f.readlines()
            count = sum(1 for line in lines if line.strip().startswith('Creating events.tsv '))
            if count < 1:
                print(f"{out_file} has {count} lines starting with 'Creating events.tsv ' (expected 1).")

def check_prfanalyze_errs(folder_path):
    # Find all .out files in the folder
    err_files = [f for f in os.listdir(folder_path) if f.endswith('.err') or f.endswith('.e')]
    if not err_files:
        print('No .err files found in the folder.')
        return

    allowed_prefixes = ("INFO:", "real", "user", "sys")
    for err_file in err_files:
        err_file_path = os.path.join(folder_path, err_file)
        
        with open(err_file_path, "r") as f:
            lines = f.readlines()
        error_lines = []
        for line in lines:
            stripped_line = line.strip()
            if not stripped_line:
                continue
            if not stripped_line.startswith(allowed_prefixes):
                error_lines.append(stripped_line)
        if error_lines:
            print(f"\n {err_file} has problems ")
            print(error_lines)

def check_prfanalyze_logs(folder_path):
    # Find all .out files in the folder
    out_files = [f for f in os.listdir(folder_path) if f.endswith('.out') or f.endswith('.o')]
    if not out_files:
        print('No .out files found in the folder.')
        return

    for out_file in out_files:
        out_file_path = os.path.join(folder_path, out_file)
        count=0
        with open(out_file_path) as f:
            lines = f.readlines()
            count = sum(1 for line in lines if line.strip().startswith('Writing the estimates'))
            if count != 6:
                print(f"\n{out_file} has {count} lines starting with 'Writing the estimates' (expected 6).")

# Example usage:
folder = '/scratch/tlei/VOTCLOC/dipc_prfprepare_logs/march29'

check_prfprepare_logs(folder)

#check_prfanalyze_errs(folder)
#check_prfanalyze_logs(folder)