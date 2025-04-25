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

step=presurfer
basedir=/bcbl/home/public/Gari/VOTCLOC/main_exp
bids_dirname=BIDS

src_dir=$basedir/raw_nifti
analysis_name=check_sub0709
outputdir=${basedir}/${bids_dirname}
force=false # if overwrite exsting file

codedir=$basedir/code/01_prepare_nifti
subseslist_path=$codedir/subseslist_presurfer.txt
script_dir=/export/home/tlei/tlei/soft/launchcontainers/src/launchcontainers/py_pipeline/01_prepare_nifti/prepare_anat
logdir=${outputdir}/log_${step}/${analysis_name}_$(date +"%Y-%m-%d")
echo "The logdir is $logdir"
echo "The outputdir is $outputdir"
mkdir -p $logdir


echo "reading the subses"
# Initialize a line counter
line_number=0
# Read the file line by line
while IFS=$'\t' read -r sub ses
do
    echo "line number is $line_number sub is $sub ses is $ses"
    # Increment line counter
    ((line_number++))

    # Skip the first line which is the header
    if [ $line_number -eq 1 ]; then
        continue
    fi

	echo this is line number $line_number
	echo "### CONVERTING TO NIFTI OF SUBJECT: $sub $ses SESSION ###"

    # Define the name of logs
    now=$(date +"%H;%M")
    log_file="${logdir}/presurfer_${sub}_${ses}_${now}.o"
    error_file="${logdir}/presurfer_${sub}_${ses}_${now}.e"
	# Export variables for use in the called script
    export src_dir
    export outputdir
    export sub
    export ses
    export force
    export script_dir
    # Command to execute locally
    cmd="bash $script_dir/run_${step}.sh "

    # Run the command in the background
    echo $cmd
    eval $cmd > ${log_file} 2> ${error_file}
    unset sub
    unset ses
done < "$subseslist_path"
