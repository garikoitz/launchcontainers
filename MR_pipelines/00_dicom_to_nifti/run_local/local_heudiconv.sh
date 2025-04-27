# """
# MIT License

# Copyright (c) 2024-2025 Yongning Lei

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial
# portions of the Software.
# """
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
