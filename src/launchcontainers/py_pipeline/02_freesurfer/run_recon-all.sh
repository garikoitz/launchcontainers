## -----------------------------------------------------------------------------
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
# BIDS ID
sub=$1
ses=$2

surfdir="/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS/derivatives/freesurfer"

export SUBJECTS_DIR=${surfdir}

###########################################################
###########################################################
###########################################################
module load freesurfer/7.3.2

basedir=/bcbl/home/public/Gari/VOTCLOC/main_exp
bids_dirname=BIDS
T1_path="$basedir/$bids_dirname/sub-$sub/ses-$ses/anat/sub-${sub}_ses-${ses}_run-01_T1w.nii.gz"
outputdir=${basedir}/${bids_dirname}
step=reconall
analysis_name=check0709
logdir=${outputdir}/log_${step}/${analysis_name}_$(date +"%Y-%m-%d")
echo "The logdir is $logdir"
echo "The outputdir is $outputdir"
mkdir -p $logdir

now=$(date +"%H;%M")
log_file="${logdir}/reconall_${sub}_${ses}_${now}.o"
error_file="${logdir}/reconall_${sub}_${ses}_${now}.e"

cmd="recon-all -i ${T1_path} \
          -subjid sub-${sub} \
          -sd ${basedir}/${bids_dirname}/derivatives/freesurfer \
          -all "

echo "Going to run recon-all on sub-${sub}"
echo $cmd
eval $cmd > ${log_file} 2> ${error_file}
