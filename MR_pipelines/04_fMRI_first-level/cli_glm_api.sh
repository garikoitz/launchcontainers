echo "Subject: ${sub} "
echo "Session: ${ses} "
echo "We are going to use smoothed?  ${use_smoothed}"
echo "the smoothing on bold is ${sm} fwhm"
echo "Task we are running are: ${task}"
echo "get the codedir": ${codedir}
echo "Start scans is ": ${start_scans}
echo "The mode is dry_run" ${dry_run}
echo "output name is ${out_name}"
source ~/tlei/soft/miniconda3/etc/profile.d/conda.sh
conda activate votcloc
echo "going to run python"



if [ "$dry_run" = "True" ]; then
    cmd_nosmooth="python ${codedir}/run_glm.py \
-base ${basedir} -sub ${sub} -ses ${ses} -fp_ana_name ${fp_name} \
-task ${task} -start_scans ${start_scans} -space ${space} -contrast ${glm_yaml_path} \
-output_name ${out_name} \
-slice_time_ref ${slice_timing} -dry_run "
else
    cmd_nosmooth="python ${codedir}/run_glm.py \
-base ${basedir} -sub ${sub} -ses ${ses} -fp_ana_name ${fp_name} \
-task ${task} -start_scans ${start_scans} -space ${space} -contrast ${glm_yaml_path} \
-output_name ${out_name} \
-slice_time_ref ${slice_timing} "
fi


# -selected_runs 5 6 \
echo $cmd_nosmooth
eval $cmd_nosmooth

