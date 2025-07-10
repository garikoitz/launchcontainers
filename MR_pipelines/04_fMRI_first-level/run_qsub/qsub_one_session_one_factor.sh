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
analysis='beforeMar05_US' #'beforeMar05_US' #runall_US

codedir=/export/home/tlei/tlei/soft/VOTCLOC/MR_analysis/05_fROI/analysis_rand_run

#subseslist_path=$codedir/subseslist.tsv
subseslist_path=$basedir/$bids_dir_name/code/subseslist_${analysis}.txt

fp_name=${analysis}
out_name="rand_run_Mar17"
slice_timing=(0.5)
LOG_DIR=$basedir/$bids_dir_name/derivatives/l1_surfaces_log/analysis-${out_name}


sub=$1
ses=$2
factor=$3
mkdir -p "$LOG_DIR"

use_smoothed=False
# Read the file line by line

current_time=$(date +"%Y-%m-%d_%H-%M-%S")

# Define log paths
qsub_log_out="${LOG_DIR}/f${factor}_s${sub}_t${ses}_${current_time}.o"
qsub_log_err="${LOG_DIR}/f${factor}_s${sub}_t${ses}_${current_time}.e"

echo "### Runing SURFACE_glm on SUBJECT: $sub $ses SESSION ###"
cmd="qsub -q short.q \
	-N run${factor}-${sub}T${ses} \
	-o $qsub_log_out \
	-e $qsub_log_err \
	-l mem_free=8G \
	-v basedir=${basedir} \
	-v sub=${sub} \
	-v ses=${ses} \
	-v fp_name=${fp_name} \
	-v out_name=${out_name} \
	-v factor=${factor} \
	-v slice_timing=${slice_timing} \
	-v codedir=$codedir \
	$codedir/run_python_glm.sh "

echo $cmd
eval $cmd
