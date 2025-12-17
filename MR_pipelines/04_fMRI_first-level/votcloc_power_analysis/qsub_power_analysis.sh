#!/bin/bash
# -----------------------------------------------------------------------------
# Copyright (c) Yongning Lei 2024
# qsub_power_analysis.sh - Submit power analysis jobs (one job per subject/session)
# -----------------------------------------------------------------------------

basedir=/bcbl/home/public/Gari/VOTCLOC/main_exp
bids_dir_name=BIDS

# directory stores the run_glm.py code
codedir=/export/home/tlei/tlei/soft/launchcontainers/MR_pipelines/04_fMRI_first-level
# analysis name of fmriprep
fp_name=25.1.4_t2-fs_dummyscans-5_bold2anat-t2w_forcebbr
task=fLoc
# start_scans: number of scans started:
start_scans=6
# space
space=fsnative

# output analysis name
out_name=final_v2
# path to contrast yaml
glm_yaml_path=${codedir}/contrast_lexper.yaml
# slice timing ref, default is 0.5
slice_timing=0.5
use_smoothed=False
dry_run=False

# Power analysis parameters
total_runs=10
n_iterations=10
seed=42

# log dir
LOG_DIR=$basedir/l1_surfaces_log/analysis-${out_name}_power_analysis

subseslist_name=$1
subseslist_path=$basedir/code/$subseslist_name

mkdir -p "$LOG_DIR"

echo "=================================================="
echo "Submitting Power Analysis Jobs"
echo "Each job will run 100 GLMs (10 iterations × 10 run configs)"
echo "=================================================="
echo ""

# Initialize a line counter
line_number=0

# Read the file line by line
while IFS=$',' read -r sub ses RUN _;
do
    # Increment line counter
    ((line_number++))

    # Skip the first line which is the header
    if [ $line_number -eq 1 ]; then
        continue
    fi
    
    current_time=$(date +"%Y-%m-%d_%H-%M-%S")
    
    if [ "$RUN" = "True" ]; then
        # Define log paths
        qsub_log_out="${LOG_DIR}/sub-${sub}_ses-${ses}_${current_time}.o"
        qsub_log_err="${LOG_DIR}/sub-${sub}_ses-${ses}_${current_time}.e"

        echo "### Submitting Power Analysis for SUBJECT: $sub SESSION: $ses ###"
        
        cmd="qsub -q long.q \
            -N PA_S${sub}_T${ses} \
            -o $qsub_log_out \
            -e $qsub_log_err \
            -l mem_free=25G \
            -v basedir=${basedir} \
            -v sub=${sub} \
            -v ses=${ses} \
            -v fp_name=${fp_name} \
            -v task=${task} \
            -v start_scans=${start_scans} \
            -v space=${space} \
            -v out_name=${out_name} \
            -v glm_yaml_path=${glm_yaml_path} \
            -v slice_timing=${slice_timing} \
            -v total_runs=${total_runs} \
            -v n_iterations=${n_iterations} \
            -v seed=${seed} \
            -v codedir=${codedir} \
            -v dry_run=${dry_run} \
            ${codedir}/votcloc_power_analysis/cli_glm_power_analysis.sh"

        echo $cmd
        eval $cmd
    fi
done < "$subseslist_path"

echo ""
echo "=================================================="
echo "All jobs submitted!"
echo "=================================================="