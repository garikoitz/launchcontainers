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

analysis_name=check_sub0709
logdir=${outputdir}/log_heudiconv/${analysis_name}_$(date +"%Y-%m-%d")/${step}
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
	now=$(date +"%H-%M")
	log_file="${logdir}/qsub_${sub}_${ses}_${now}.o"
    error_file="${logdir}/qsub_${sub}_${ses}_${now}.e"
	cmd="qsub -q short.q \
	    -S /bin/bash \
		-N heudiconv_s-${sub}_s-${ses} \
		-o $log_file \
    	-e $error_file \
		-l mem_free=16G \
		-v basedir=${basedir},logdir=${logdir},dicom_dirname=$dicom_dirname,outputdir=${outputdir},sub=${sub},ses=${ses},heuristicfile=$heuristicfile,sing_path=$sing_path \
		$script_dir/src_heudiconv_${step}.sh "

	echo $cmd
	eval $cmd
done < "$subseslist_path"

cp "$0" "$logdir"
cp "$script_dir/src_heudiconv_${step}.sh" "$logdir"
