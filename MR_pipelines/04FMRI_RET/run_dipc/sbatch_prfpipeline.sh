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
mem="8G"
cpus="6"
time="00:10:00"
task="all"

# # # for prfanalyze-vista:
# step="prfanalyze-vista"
# version='2.2.1'
# qos="regular" # regular or test
# mem="32G"
# cpus="20"
# time="10:00:00" #time="00:10:00" 10:00:00
# task="retFF" # retCB retRW retFF


# # # for prfresult:
# step="prfresult"
# version="0.1.1"
# qos="test" # regular or test
# mem="16G"
# cpus="10"
# time="00:10:00" #time="00:10:00" 10:00:00
# task="all" # retCB retRW retFF

# json input
json_dir="$baseP/code/${step}_jsons"
# subseslist dir:
script_dir="/scratch/tlei/soft/launchcontainers/src/launchcontainers/py_pipeline/04b_prf"
code_dir=$baseP/code/04b_prf
subses_list_dir=$code_dir/subseslist_prfnormal.txt
sif_path="/scratch/tlei/containers/${step}_${version}.sif"

# log dir
LOG_DIR="$baseP/dipc_${step}_logs/edr25ses_$(date +"%Y-%m-%d")"
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
