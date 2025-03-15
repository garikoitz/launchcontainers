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

# Define base paths
baseP="/bcbl/home/public/Gari/VOTCLOC/main_exp"
HOME_DIR="$baseP/singularity_home"
# container specific
# for prfprepare:
license_dir="/export/home/tlei/tlei/linux_settings"
step="prfprepare"
version="1.5.0"
queue= "short.q"
mem= "16G"

# # for prfanalyze-vista:
# step="prfanalyze-vista"
# version='2.2.1'
# queue= "long.q"
# mem= "32G"

# # for prfresult:
# step="prfresult"
# version="0.1.1"
# queue= "short.q"
# mem= "16G"


# subseslist dir:
code_dir="/export/home/tlei/tlei/soft/launchcontainers/src/launchcontainers/py_pipeline/04b_prf"
subses_list_dir=$code_dir/subseslist_votcloc.txt


# log dir
LOG_DIR="$baseP/BIDS/derivatives/${step}/${step}_logs/qsub_may14"
# json input
json_dir="/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS/code/${step}_jsons"
# Ensure directories exist
mkdir -p "$LOG_DIR"
mkdir -p "$HOME_DIR"

# Read subseslist.txt (Skipping header line)
tail -n +2 $subses_list_dir | while read sub ses; do
    current_time=$(date +"%Y-%m-%d_%H-%M-%S")

    # Define log paths
    qsub_log_out="${LOG_DIR}/qsub_sub-${sub}_ses-${ses}_${current_time}.o"
    qsub_log_err="${LOG_DIR}/qsub_sub-${sub}_ses-${ses}_${current_time}.e"

    # Construct qsub command
	# if it is prepare and result, we use short.q, otherwise, long.q and more ram
    cmd="qsub -q short.q \
        -S /bin/bash \
        -M t.lei@bcbl.eu \
        -N ${step}_s-${sub}_t-${ses} \
        -o $qsub_log_out \
        -e $qsub_log_err \
        -l mem_free=16G \
        -v baseP=${baseP},LOG_DIR=${LOG_DIR},license_dir=${license_dir},version=${version},sub=${sub},ses=${ses},json_dir=$json_dir \
        $code_dir/${step}.sh"

    # Print and execute the command
    echo "Submitting job for sub-${sub} ses-${ses}"
    echo $cmd
    eval $cmd

done
