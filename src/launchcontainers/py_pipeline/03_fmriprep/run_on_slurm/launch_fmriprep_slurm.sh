STUDY="/scratch/tlei/VOTCLOC"
code_dir="/scratch/tlei/soft/MRIworkflow/03_fmriprep"
# /scratch/tlei/soft/MRIworkflow/03_fmriprep/fmriprep.slurm

TOTAL_LINES=$(wc -l < "${STUDY}/code/subseslist_Jan31.txt")

echo "Total lines is $TOTAL_LINES"

DATA_LINES=$((TOTAL_LINES - 1))

sbatch --array=1-${DATA_LINES} ${code_dir}/run_on_slurm/fmriprep.slurm 

## Attention!  the sublist needs to be subject only, and all the session will be processed in the same time
## if want to specify session, then specify the bids_filter.json file,