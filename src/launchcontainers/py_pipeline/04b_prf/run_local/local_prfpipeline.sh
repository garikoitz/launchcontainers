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
license_path="$baseP/BIDS/.license"

##### For each container
#####
# step="prfprepare"
# version="1.5.0"
# queue="veryshort.q"
# mem="8G"
# cpus="6"
# time="00:10:00"
# task="all"

# # # for prfanalyze-vista:
# step="prfanalyze-vista"
# version='2.2.1'
# queue="long.q"
# mem="32G"
# cpus="20"
# time="10:00:00" #time="00:10:00" 10:00:00
# task="retFF" # retCB retRW retFF

# # for prfresult:
step="prfresult"
version="0.1.3"
queue="short.q"
mem="16G"
cpus="10"
time="02:00:00" #time="00:10:00" 10:00:00
task="all" # retCB retRW retFF

# json input
json_dir="$baseP/code/${step}_jsons"
# subseslist dir:
script_dir="/export/home/tlei/tlei/soft/launchcontainers/src/launchcontainers/py_pipeline/04b_prf"
code_dir=$baseP/code/04b_prf
subses_list_dir=$code_dir/subseslist_votcloc.txt
sif_path="/bcbl/home/public/Gari/singularity_images/${step}_${version}.sif"

# log dir
LOG_DIR="$baseP/ips_${step}_logs/hyperion20ses_$(date +"%Y-%m-%d")"
# Ensure directories exist
mkdir -p "$LOG_DIR"
mkdir -p "$HOME_DIR"

line_num=1
# Read subseslist.txt (Skipping header line)
tail -n +2 "$subses_list_dir" | while read sub ses; do
    ((line_num++))

    now=$(date +"%H-%M")
	log_file="${LOG_DIR}/local_${sub}_${ses}_${now}.o"
    error_file="${LOG_DIR}/local_${sub}_${ses}_${now}.e"

    # Export variables for the script
    export baseP="$baseP"
    export license_path="$license_path"
    export version="$version"
    export sub="$sub"
    export ses="$ses"
    export json_path="$json_dir/${task}_sub-${sub}_ses-${ses}.json"
    export sif_path="$sif_path"

    # Info
    echo "Running job locally for sub-${sub} ses-${ses}"
    echo "Logging to:"
    echo "  STDOUT: $log_file"
    echo "  STDERR: $error_file"

    # Run the script in background and log output
    cmd="bash $script_dir/run_local/${step}_local.sh" >"$log_file" 2>"$error_file"
    echo "###***#*#*#*##*$cmd"
    eval $cmd
done
