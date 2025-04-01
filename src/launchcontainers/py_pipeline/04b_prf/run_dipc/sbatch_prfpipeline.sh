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
baseP="/scratch/tlei/VOTCLOC"

HOME_DIR="$baseP/singularity_home"
# container specific
# for prfprepare:
license_path="$baseP/BIDS/.license"



##### For each container
#####
# step="prfprepare"
# version="1.5.0"
# qos="regular" # or test or regular
# mem="8G"
# cpus="6"
# time="00:10:00"
# task="all"

# # # for prfanalyze-vista:
# step="prfanalyze-vista"
# version='2.2.1'
# qos="regular" # regular or test
# mem="32G"
# cpus="20"
# time="10:00:00" #time="00:10:00" 10:00:00
# task="retFF" # retCB retRW retFF


# # for prfresult:
step="prfresult"
version="0.1.1"
qos="test" # regular or test
mem="16G"
cpus="10"
time="00:10:00" #time="00:10:00" 10:00:00
task="all" # retCB retRW retFF

# json input
json_dir="$baseP/code/${step}_jsons"
# subseslist dir:
script_dir="/scratch/tlei/soft/launchcontainers/src/launchcontainers/py_pipeline/04b_prf"
code_dir=$baseP/code
subses_list_dir=$code_dir/subseslist_votcloc.txt
sif_path="/scratch/tlei/containers/${step}_${version}.sif"

# log dir
LOG_DIR="$baseP/dipc_${step}_logs/hyperion20ses_$(date +"%Y-%m-%d")"
# Ensure directories exist
mkdir -p "$LOG_DIR"
mkdir -p "$HOME_DIR"

line_num=1
# Read subseslist.txt (Skipping header line)
tail -n +2 $subses_list_dir | while read sub ses; do
    ((lin_num++))
    now=$(date +"%H-%M")
    # Construct sbatch command
	# if it is prepare and result, we use short.q, otherwise, long.q and more ram
    cmd="sbatch -J ${lin_num}_${task}_${step} \
        --time=${time} \
        -n 1 \
        --cpus-per-task=${cpus} \
        --mem=${mem} \
        --partition=general \
        --qos=${qos} \
        -o "$LOG_DIR/%J_%x_${sub}-${ses}_${now}.o" \
        -e "$LOG_DIR/%J_%x_${sub}-${ses}_${now}.e" \
        --export=ALL,baseP=${baseP},license_path=${license_path},version=${version},sub=${sub},ses=${ses},json_path=$json_dir/${task}_sub-${sub}_ses-${ses}.json,sif_path=$sif_path \
        $script_dir/run_dipc/${step}_dipc.sh "

    # Print and execute the command
    echo "Submitting job for sub-${sub} ses-${ses}"
    echo $cmd
    eval $cmd

done
