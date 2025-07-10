echo "Subject: ${sub} "
echo "Session: ${ses} "
echo "We are going to use smoothed?  ${use_smoothed}"
echo "the smoothing on bold is ${sm} fwhm"
echo "get the codedir": ${codedir}
source ~/tlei/soft/miniconda3/etc/profile.d/conda.sh
conda activate votcloc
echo "going to run python"


cmd_nosmooth="python ${codedir}/run_glm.py \
-base ${basedir} -sub ${sub} -ses ${ses} -fp_ana_name ${fp_name} \
-task fLoc -space fsnative -contrast ${glm_yaml_path} \
-output_name ${out_name} \
-slice_time_ref ${slice_timing} "

echo $cmd_nosmooth
eval $cmd_nosmooth
