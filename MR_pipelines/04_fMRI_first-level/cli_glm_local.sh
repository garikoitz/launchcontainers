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
codedir=/export/home/tlei/tlei/soft/VOTCLOC/MR_analysis/04_surface_glm

# analysis name of fmriprep #'beforeMar05_US' there is only one analysis now
fp_name=beforeMar05_US

# path to contrast yaml, you can define any kind of yaml under any place
glm_yaml_path=${codedir}/contrast_wordcenter.yaml
# slice timing ref, default is 0.5 can change
slice_timing=(0.5)
use_smoothed=False

# cli input of sub and ses
sub=$1
ses=$2
# task id in the BIDS format
task=$3
# number of dummy scans
start_scans=$4
# output analysis name
out_name=$5

# log dir
LOG_DIR=$basedir/l1_surfaces_log/analysis-${out_name}
mkdir -p "$LOG_DIR"

current_time=$(date +"%Y-%m-%d_%H-%M-%S")

# Define log paths
log_out="${LOG_DIR}/sub-${sub}_ses-${ses}_${current_time}.o"
log_err="${LOG_DIR}/sub-${sub}_ses-${ses}_${current_time}.e"

echo "Subject: ${sub} "
echo "Session: ${ses} "
echo "We are going to use smoothed?  ${use_smoothed}"
echo "the smoothing on bold is ${sm} fwhm"
echo "Task we are running are: ${task}"
echo "get the codedir": ${codedir}
echo "Start scans is ": ${start_scans}
source ~/tlei/soft/miniconda3/etc/profile.d/conda.sh
conda activate votcloc
echo "going to run python"


cmd_nosmooth="python ${codedir}/run_glm.py \
-base ${basedir} -sub ${sub} -ses ${ses} -fp_ana_name ${fp_name} \
-task ${task} -start_scans ${start_scans} -space fsnative -contrast ${glm_yaml_path} \
-output_name ${out_name} \
-selected_runs 5 6 \
-slice_time_ref ${slice_timing} "

echo $cmd_nosmooth
read -p "Do you want to run this command? (y/n): " answer
case "$answer" in
    [Yy]* )
        echo "Running command..."
        eval $cmd_nosmooth  > $log_out 2> $log_err
        ;;
    [Nn]* )
        echo "Command not executed."
        ;;
    * )
        echo "Invalid input. Aborting."
        exit 1
        ;;
esac
