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
unset project

step=$1 # step1 or step2
project=$2 # votcloc
basedir=/bcbl/home/public/Gari/VOTCLOC/main_exp
dicom_dirname=dicom
outputdir=$basedir/raw_nifti

codedir=/export/home/tlei/tlei/soft/MRIworkflow/00_dicom_to_nifti/project_$project
subseslist_path=$codedir/subseslist_${project}.txt
heuristicfile=$codedir/heuristic/heuristic_${project}.py
sing_path=/bcbl/home/public/Gari/singularity_images

analysis_name=wordcenter_ret
logdir=${outputdir}/log_heudiconv/$analysis_name/${step}_${project}
echo "The logdir is $logdir"
echo "The outputdir is $outputdir"
mkdir -p $logdir


echo "reading the subses"
# Initialize a line counter
line_number=0
# Read the file line by line
#!/bin/bash

# Initialize line number counter
line_number=0
max_jobs=10  # Set the maximum number of parallel jobs

# Loop through the subseslist
while IFS=$'\t' read -r sub ses; do
    echo "line number is $line_number sub is $sub ses is $ses"
    ((line_number++))  # Increment line counter

    # Skip the first line (header)
    if [ $line_number -eq 1 ]; then
        continue
    fi

    echo $line_number 
    echo "### CONVERTING TO NIFTI OF SUBJECT: $sub SESSION: $ses  ###"

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
    cmd="bash $codedir/src_heudiconv_${step}_${project}.sh"

    now=$(date +"%Y-%m-%dT%H:%M")    
    log_file="${logdir}/heudiconv_${sub}_${ses}_${step}_${now}.log"
    error_file="${logdir}/heudiconv_${sub}_${ses}_${step}_${now}.err"
    # Run the command in the background
    echo $cmd
    eval $cmd > ${log_file} 2> ${error_file}

done < "$subseslist_path"


