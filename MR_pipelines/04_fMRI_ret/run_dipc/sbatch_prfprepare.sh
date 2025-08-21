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
baseP="/scratch/tlei/VOTCLOC"

HOME_DIR="$baseP/singularity_home"
# container specific
# for prfprepare:
license_path="$baseP/BIDS/.license"



##### For each container
#####
step="prfprepare"
version="1.5.0"
qos="regular" # or test or regular
mem="16G"
cpus="6"
time="00:30:00"
task="all"

# json input
json_dir="$baseP/code/${step}_jsons"
# subseslist dir:
script_dir="/scratch/tlei/soft/launchcontainers/MR_pipelines/04_fMRI_ret"
code_dir=$baseP/code
subses_list_dir=$code_dir/subseslist_jun16.txt
sif_path="/scratch/tlei/containers/${step}_${version}.sif"
log_note=$1
# log dir
LOG_DIR="$baseP/dipc_${step}_logs/${log_note}_$(date +"%Y-%m-%d")"
# Ensure directories exist
mkdir -p "$LOG_DIR"
mkdir -p "$HOME_DIR"

line_num=1
# Read subseslist.txt (Skipping header line)
tail -n +2 $subses_list_dir | while IFS=',' read -r sub ses _; do
    ((lin_num++))
    now=$(date +"%H-%M")
    # Construct sbatch command
	# if it is prepare and result, we use short.q, otherwise, long.q and more ram
    cmd="sbatch -J ${lin_num}_${task}_${step} \
        --time=${time} \
        -n 1 \
        --cpus-per-task=${cpus} \
        --mem=${mem} \
        --partition=general \
        --qos=${qos} \
        -o "$LOG_DIR/%J_%x_${sub}-${ses}_${now}.o" \
        -e "$LOG_DIR/%J_%x_${sub}-${ses}_${now}.e" \
        --export=ALL,baseP=${baseP},license_path=${license_path},version=${version},sub=${sub},ses=${ses},json_path=$json_dir/${task}_sub-${sub}_ses-${ses}.json,sif_path=$sif_path \
        $script_dir/run_dipc/${step}_dipc.sh "

    # Print and execute the command
    echo "Submitting job for sub-${sub} ses-${ses}"
    echo $cmd
    eval $cmd

done
