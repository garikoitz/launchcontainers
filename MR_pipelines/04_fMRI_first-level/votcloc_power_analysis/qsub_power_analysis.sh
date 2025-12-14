#!/bin/bash
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

basedir=/bcbl/home/public/Gari/VOTCLOC/main_exp
bids_dir_name=BIDS

# directory stores the run_glm.py code
codedir=/export/home/tlei/tlei/soft/launchcontainers/MR_pipelines/04_fMRI_first-level
# analysis name of fmriprep
fp_name=25.1.4_t2-fs_dummyscans-5_bold2anat-t2w_forcebbr
task=fLoc
# start_scans: number of scans started:
start_scans=6
# space
space=fsnative

# Base output analysis name
base_out_name=final_v2
# path to contrast yaml
glm_yaml_path=${codedir}/contrast_lexper.yaml
# slice timing ref, default is 0.5
slice_timing=0.5
use_smoothed=False
dry_run=False

# Directory where run_list files are stored
run_list_dir=${basedir}/code

# Subject-session list
subseslist_name=$1
subseslist_path=${basedir}/code/${subseslist_name}

# Base log directory
BASE_LOG_DIR=${basedir}/l1_surfaces_log

echo "=================================================="
echo "Starting power analysis job submission"
echo "Base directory: ${basedir}"
echo "Code directory: ${codedir}"
echo "Subject-session list: ${subseslist_path}"
echo "=================================================="
echo ""

# Loop through num_of_runs from 1 to 10
for num_of_runs in {1..9}; do
    
# Define run combinations file
# do 1 run launch
# num_of_runs=1
run_combinations="${run_list_dir}/run_list_${num_of_runs}_run.txt"

# Check if file exists
if [ ! -f "$run_combinations" ]; then
	echo "WARNING: ${run_combinations} not found, skipping ${num_of_runs} runs..."
	echo ""
	continue
fi

# Read run combinations into array
mapfile -t run_lists < "$run_combinations"
n_iterations=${#run_lists[@]}

# Create output name for this num_of_runs
out_name="${base_out_name}/power_analysis_${num_of_runs}_run"

# Create log directory for this configuration
LOG_DIR="${BASE_LOG_DIR}/analysis-${out_name}"
mkdir -p "$LOG_DIR"

echo "=================================================="
echo "Processing: ${num_of_runs} run(s) per iteration"
echo "Iterations: ${n_iterations}"
echo "Run list file: ${run_combinations}"
echo "Output name: ${out_name}"
echo "=================================================="
echo ""

# Initialize a line counter
line_number=0

# Read subject-session list
while IFS=$',' read -r sub ses RUN _; do
	# Increment line counter
	((line_number++))
	
	# Skip the first line which is the header
	if [ $line_number -eq 1 ]; then
		continue
	fi
	
	# Skip if RUN is not True
	if [ "$RUN" != "True" ]; then
		continue
	fi
	
	echo "Subject: ${sub}, Session: ${ses} (${num_of_runs} runs)"
	
	# Loop through each run combination (iteration)
	for iter_idx in "${!run_lists[@]}"; do
		iter_num=$((iter_idx + 1))
		selected_runs="${run_lists[$iter_idx]}"
		
		# Get current timestamp
		current_time=$(date +"%Y-%m-%d_%H-%M-%S")
		
		# Define log paths
		qsub_log_out="${LOG_DIR}/sub-${sub}_ses-${ses}_iter${iter_num}_${current_time}.o"
		qsub_log_err="${LOG_DIR}/sub-${sub}_ses-${ses}_iter${iter_num}_${current_time}.e"
		
		echo "  Iteration ${iter_num}: runs [${selected_runs}]"
		
		# Submit job
		cmd="qsub -q long.q \
			-N S${sub}_T${ses}_${num_of_runs}r_i${iter_num} \
			-o $qsub_log_out \
			-e $qsub_log_err \
			-l mem_free=20G \
			-v basedir=${basedir} \
			-v sub=${sub} \
			-v ses=${ses} \
			-v fp_name=${fp_name} \
			-v task=${task} \
			-v start_scans=${start_scans} \
			-v space=${space} \
			-v out_name=${out_name} \
			-v glm_yaml_path=${glm_yaml_path} \
			-v slice_timing=${slice_timing} \
			-v selected_runs=\"${selected_runs}\" \
			-v codedir=${codedir} \
			-v dry_run=${dry_run} \
			${codedir}/cli_glm_api.sh"
		
		#echo "$cmd"
		eval $cmd
	done
	
	echo ""
	
done < "$subseslist_path"

# Reset line counter for next num_of_runs
line_number=0

echo ""
    
done

echo "=================================================="
echo "All jobs submitted!"
echo "Total configurations: 1-10 runs"
echo "Iterations per configuration: 10"
echo "=================================================="