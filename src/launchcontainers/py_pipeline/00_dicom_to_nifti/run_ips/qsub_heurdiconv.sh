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
unset step

step=$1 # step1 or step2
project=votcloc
basedir=/bcbl/home/public/Gari/VOTCLOC/main_exp
dicom_dirname=dicom
outputdir=$basedir/raw_nifti

codedir=$basedir/BIDS/code
script_dir=/export/home/tlei/tlei/soft/launchcontainers/src/launchcontainers/py_pipeline/00_dicom_to_nifti
subseslist_path=$codedir/00_heudiconv/subseslist_${project}.txt
heuristicfile=$codedir/00_heudiconv/heuristic_${project}.py
sing_path=/bcbl/home/public/Gari/singularity_images/heudiconv_1.3.2.sif

analysis_name=check_sub0709
logdir=${outputdir}/log_heudiconv/$analysis_name_$(date +"%Y-%m-%d")/${step}
echo "The logdir is $logdir"
echo "The outputdir is $outputdir"
mkdir -p $logdir

echo "reading the subses"
# Initialize a line counter
line_number=0
# Read the file line by line
while IFS=$'\t' read -r sub ses; do
    echo "line number is $line_number sub is $sub ses is $ses"
    # Increment line counter
    ((line_number++))

    # Skip the first line which is the header
    if [ $line_number -eq 1 ]; then
        continue
    fi

	echo "### CONVERTING TO NIFTI OF SUBJECT: $sub $ses SESSION ###"
	now=$(date +"%H:%M")
	log_file="${logdir}/qsub_${sub}_${ses}_${now}.o"
    error_file="${logdir}/qsub_${sub}_${ses}_${now}.e"
	cmd="qsub -q short.q \
	    -S /bin/bash \
		-N heudiconv_s-${sub}_s-${ses} \
		-o $log_file \
    	-e $error_file \
		-l mem_free=16G \
		-v basedir=${basedir} \
		-v logdir=${logdir} \
		-v dicom_dirname=$dicom_dirname \
		-v outputdir=${outputdir} \
		-v sub=${sub} \
		-v ses=${ses} \
		-v heuristicfile=$heuristicfile \
		-v sing_path=$sing_path \
		$codedir/src_heudiconv_${step}_${project}.sh "

	echo $cmd
	eval $cmd
done < "$subseslist_path"

cp "$0" "$logdir/qsub_heudiconv${step}_${project}"
cp "$script_dir/src_heudiconv_${step}.sh" "$logdir"
