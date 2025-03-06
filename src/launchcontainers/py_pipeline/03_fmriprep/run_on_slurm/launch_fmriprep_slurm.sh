unset analysis_name

STUDY="/scratch/tlei/VOTCLOC"
code_dir="/scratch/tlei/soft/launchcontainers/src/launchcontainers/py_pipeline/03_fmriprep"

analysis_name='runall_US'
export analysis_name

slurm_log_dir=/scratch/tlei/VOTCLOC/logs_slurm/log_slurm-$analysis_name
mkdir -p ${slurm_log_dir}

TOTAL_LINES=$(wc -l < "${STUDY}/BIDS/code/subseslist.txt")

echo "Total lines is $TOTAL_LINES"

DATA_LINES=$((TOTAL_LINES - 1))

cmd="sbatch 
    --export=ALL,analysis_name="${analysis_name}",slurm_log_dir="${slurm_log_dir}" \
    --array=1-${DATA_LINES} \
    -o "$slurm_log_dir/%J_%x-%A-%a.out" \
    -e "$slurm_log_dir/%J_%x-%A-%a.err" \
    ${code_dir}/run_on_slurm/fmriprep.slurm "

## Attention! the sublist needs to be subject only, and all the session will be processed in the same time
## if want to specify session, then specify the bids_filter.json file,
echo " The command for the slurm is "
echo $cmd
eval $cmd

