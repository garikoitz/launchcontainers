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
# analysis name of fmriprep #'beforeMar05_US'  or 25.1.4_t2_pial_dummyscan_5
fp_name=25.1.4_t2-fs_dummyscans-5_bold2anat-t2w_forcebbr
task=fLoc
# start_scans: number of scans started:
start_scans=6
# space
space=fsnative
# output analysis name
out_name=final_v2
# path to contrast yaml, you can define any kind of yaml under any place
glm_yaml_path=${codedir}/contrast_votcloc_all.yaml
# slice timing ref, default is 0.5 can change
slice_timing=(0.5)
use_smoothed=False
dry_run=False
# log dir
LOG_DIR=$basedir/l1_surfaces_log/analysis-${out_name}

subseslist_name=$1
subseslist_path=$basedir/code/$subseslist_name



mkdir -p "$LOG_DIR"
# Initialize a line counter
line_number=0

# Read the file line by line
while IFS=$',' read -r sub ses RUN _;
do
    # Increment line counter
    ((line_number++))

    # Skip the first line which is the header
    if [ $line_number -eq 1 ]; then
        continue
    fi
	current_time=$(date +"%Y-%m-%d_%H-%M-%S")
	if [ "$RUN" = "True" ]; then
		# Define log paths
		qsub_log_out="${LOG_DIR}/sub-${sub}_ses-${ses}_${current_time}.o"
		qsub_log_err="${LOG_DIR}/sub-${sub}_ses-${ses}_${current_time}.e"


		echo "### Runing SURFACE_glm on SUBJECT: $sub $ses SESSION ###"
		cmd="qsub -q long.q \
			-N S-${sub}_T-${ses} \
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
			-v codedir=$codedir \
			-v dry_run=$dry_run \
			$codedir/cli_glm_api.sh "

		echo $cmd
		eval $cmd
	fi
done < "$subseslist_path"

