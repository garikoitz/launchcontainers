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

# Define base paths
baseP="/bcbl/home/public/Gari/VOTCLOC/main_exp"

HOME_DIR="$baseP/singularity_home"

# container specific
# for prfprepare:
license_path="$baseP/BIDS/.license"


##### For each container
#####
# step="prfprepare"
# version="1.5.0"
# queue="veryshort.q"
# mem="8G"
# cpus="6"
# time="00:10:00"
# task="all"

# # # for prfanalyze-vista:
# step="prfanalyze-vista"
# version='2.2.1'
# queue="long.q"
# mem="32G"
# cpus="20"
# time="10:00:00" #time="00:10:00" 10:00:00
# task="retFF" # retCB retRW retFF


# # # for prfresult:
step="prfresult"
version="1.0"
queue="short.q"
mem="16G"
cpus="10"
time="01:00:00" #time="00:10:00" 10:00:00
task="all" # retCB retRW retFF

# json input
json_dir="$baseP/code/${step}_jsons"

# subseslist dir:
script_dir="/export/home/tlei/tlei/soft/launchcontainers/src/launchcontainers/py_pipeline/04b_prf"
code_dir=$baseP/code
subses_list_dir=$code_dir/04b_prf/subseslist_prfnormal.txt
sif_path="/bcbl/home/public/Gari/singularity_images/${step}_${version}.sif"

# log dir
LOG_DIR="$baseP/ips_${step}_logs/make_freesurfer_$(date +"%Y-%m-%d")"
# Ensure directories exist
mkdir -p "$LOG_DIR"
mkdir -p "$HOME_DIR"

line_num=1
# Read subseslist.txt (Skipping header line)
tail -n +2 $subses_list_dir | while IFS=',' read -r sub ses _; do
    ((line_num++))
    now=$(date +"%H-%M")
	log_file="${LOG_DIR}/qsub_${sub}_${ses}_${now}.o"
    error_file="${LOG_DIR}/qsub_${sub}_${ses}_${now}.e"
    # Construct sbatch command
	# if it is prepare and result, we use short.q, otherwise, long.q and more ram
    cmd="qsub -N ${task}_${line_num}_${step} \
        -S /bin/bash \
        -q ${queue} \
        -l mem_free=${mem} \
        -o $log_file \
        -e $error_file \
        -v baseP=${baseP},license_path=${license_path},version=${version},sub=${sub},ses=${ses},json_path=$json_dir/${task}_sub-${sub}_ses-${ses}.json,sif_path=$sif_path \
        $script_dir/run_ips/${step}_ips.sh "

    # Print and execute the command
    echo "Submitting job for sub-${sub} ses-${ses}"
    echo "Job ID is $JOB_ID"
    echo $cmd
    eval $cmd

done
