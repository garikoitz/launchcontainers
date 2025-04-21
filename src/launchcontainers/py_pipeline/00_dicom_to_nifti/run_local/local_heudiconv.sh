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
#### user customize

project=paperdv
basedir=/bcbl/home/public/Gari/MINI/paper_dv
outputdir=$basedir/BIDS
dicom_dirname=dicom

#### below are not going to be changed
codedir=$basedir/code
unset step
step=$1 # step1 or step2
script_dir=/export/home/tlei/tlei/soft/launchcontainers/src/launchcontainers/py_pipeline/00_dicom_to_nifti
subseslist_path=$codedir/00_heudiconv/subseslist_heudiconv.txt
heuristicfile=$codedir/00_heudiconv/heuristic_${project}.py
sing_path=/bcbl/home/public/Gari/singularity_images/heudiconv_1.3.2.sif

analysis_name=test_1sub_paperdv
logdir=${outputdir}/log_heudiconv/${analysis_name}_$(date +"%Y-%m-%d")/${step}
echo "The logdir is $logdir"
echo "The outputdir is $outputdir"
mkdir -p $logdir


echo "reading the subses"
# Initialize a line counter
line_number=0
# Read the file line by line
# Loop through the subseslist
while IFS=$'\t' read -r sub ses; do
    echo "line number is $line_number sub is $sub ses is $ses"
    ((line_number++))  # Increment line counter

    # Skip the first line (header)
    if [ $line_number -eq 1 ]; then
        continue
    fi

    echo "### CONVERTING TO NIFTI OF SUBJECT: $sub SESSION: $ses  ###"
    now=$(date +"%H;%M")
    log_file="${logdir}/local_${sub}_${ses}_${now}.o"
    error_file="${logdir}/local_${sub}_${ses}_${now}.e"
    # Export variables for use in the called script
    export basedir
    export logdir
    export dicom_dirname
    export outputdir
    export sub
    export ses
    export heuristicfile
    export sing_path

    # Command to execute locally
    cmd="bash $script_dir/src_heudiconv_${step}.sh"

    # Run the command in the background
    echo $cmd
    eval $cmd > ${log_file} 2> ${error_file}

done < "$subseslist_path"

cp "$0" "$logdir"
cp "$script_dir/src_heudiconv_${step}.sh" "$logdir"
