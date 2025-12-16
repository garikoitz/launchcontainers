#!/bin/bash
# -----------------------------------------------------------------------------
# cli_glm_power_analysis.sh - CLI wrapper for run_glm.py in power analysis mode
# This script is called by qsub_power_analysis.sh
# -----------------------------------------------------------------------------

echo "=================================================="
echo "Starting Power Analysis GLM"
echo "Subject: ${sub}"
echo "Session: ${ses}"
echo "Task: ${task}"
echo "Total runs: ${total_runs}"
echo "Iterations per config: ${n_iterations}"
echo "Random seed: ${seed}"
echo "Output name: ${out_name}"
echo "=================================================="

source ~/tlei/soft/miniconda3/etc/profile.d/conda.sh
conda activate votcloc
echo "Conda environment activated"
echo ""

# Build command with power analysis flag
cmd="python ${codedir}/run_glm.py \
-base ${basedir} \
-sub ${sub} \
-ses ${ses} \
-fp_ana_name ${fp_name} \
-task ${task} \
-start_scans ${start_scans} \
-space ${space} \
-contrast ${glm_yaml_path} \
-output_name ${out_name} \
-slice_time_ref ${slice_timing} \
-power_analysis \
-total_runs ${total_runs} \
-n_iterations ${n_iterations} \
-seed ${seed}"

# Add dry_run flag if True
if [ "$dry_run" = "True" ]; then
    cmd="${cmd} -dry_run"
fi

echo "Running command:"
echo "$cmd"
echo ""

eval $cmd

echo ""
echo "=================================================="
echo "Power Analysis GLM completed!"
echo "=================================================="