#!/bin/bash
# -----------------------------------------------------------------------------
# cli_glm_api.sh - CLI wrapper for run_glm.py
# This script is called by qsub.sh and passes parameters to the Python GLM code
# -----------------------------------------------------------------------------

echo "Subject: ${sub}"
echo "Session: ${ses}"
echo "We are going to use smoothed? ${use_smoothed}"
echo "The smoothing on bold is ${sm} fwhm"
echo "Task we are running are: ${task}"
echo "Get the codedir: ${codedir}"
echo "Start scans is: ${start_scans}"
echo "The mode is dry_run: ${dry_run}"
echo "Output name is: ${out_name}"
echo "Selected runs: ${selected_runs}"

source ~/tlei/soft/miniconda3/etc/profile.d/conda.sh
conda activate votcloc
echo "Going to run python"

# Build base command
cmd_nosmooth="python ${codedir}/run_glm.py \
-base ${basedir} -sub ${sub} -ses ${ses} -fp_ana_name ${fp_name} \
-task ${task} -start_scans ${start_scans} -space ${space} -contrast ${glm_yaml_path} \
-output_name ${out_name} \
-slice_time_ref ${slice_timing}"

# Add selected_runs if provided
if [ -n "$selected_runs" ]; then
    cmd_nosmooth="${cmd_nosmooth} -selected_runs ${selected_runs}"
fi

# Add dry_run flag if True
if [ "$dry_run" = "True" ]; then
    cmd_nosmooth="${cmd_nosmooth} -dry_run"
fi

echo $cmd_nosmooth
eval $cmd_nosmooth