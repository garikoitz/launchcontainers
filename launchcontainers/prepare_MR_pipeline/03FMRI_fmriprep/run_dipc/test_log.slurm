
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
#!/bin/bash
#
#SBATCH -J sub05test
#SBATCH --time=00:02:00
#SBATCH -n 1  # this stands for number of task
#SBATCH --cpus-per-task=1    #6
#SBATCH --mem=2000
#SBATCH --partition=general  # Queue names you can submit to
#SBATCH --qos=test       #<regular,serial,test,long or xlong>

sublist="subseslist.txt"
BIDS_DIR="/scratch/tlei/VOTCLOC/BIDS"

OUTPUT_DIR="/derivatives/fmriprep-${analysis_name}"
LOG_DIR=$BIDS_DIR/$$OUTPUT_DIR/logs

export HOMES=/scratch/tlei/fmriprep_tmps_$analysis_name
mkdir -p ${LOG_DIR}

#LOCAL_FREESURFER_DIR="/dipc/tlei/.license"

# Prepare some writeable bind-mount points.
TEMPLATEFLOW_HOST_HOME=$HOMES/.cache/templateflow
FMRIPREP_HOST_CACHE=$HOMES/.cache/fmriprep
FMRIPREP_WORK_DIR=$HOMES/.work/fmriprep
mkdir -p ${TEMPLATEFLOW_HOST_HOME}
mkdir -p ${FMRIPREP_HOST_CACHE}
mkdir -p ${FMRIPREP_WORK_DIR}

# Prepare derivatives folder
mkdir -p ${BIDS_DIR}/${OUTPUT_DIR}

# This trick will help you reuse freesurfer results across pipelines and fMRIPrep versions
# mkdir -p ${BIDS_DIR}/derivatives/freesurfer-6.0.1
# if [ ! -d ${BIDS_DIR}/${OUTPUT_DIR}/freesurfer ]; then
#         ln -s ${BIDS_DIR}/derivatives/freesurfer-6.0.1
#         ${BIDS_DIR}/${OUTPUT_DIR}/freesurfer
#         fi

# Make sure FS_LICENSE is defined in the container.
export SINGULARITYENV_FS_LICENSE=${BIDS_DIR}/.license
# /scratch/tlei/VOTCLOC/.license
# Designate a templateflow bind-mount point
export SINGULARITYENV_TEMPLATEFLOW_HOME="/templateflow"
# SINGULARITY_CMD="unset PYTHONPATH && singularity run --cleanenv --home /scratch/glerma \
#                  -B /scratch/glerma:/scratch/glerma \
SINGULARITY_CMD="unset PYTHONPATH && singularity run --cleanenv --no-home --writable-tmpfs\
                 -B $BIDS_DIR:/base \
                 -B ${TEMPLATEFLOW_HOST_HOME}:${SINGULARITYENV_TEMPLATEFLOW_HOME}\
                 -B ${FMRIPREP_HOST_CACHE}:/work \
                 /scratch/tlei/containers/fmriprep_unstable.sif "

                 # If you already have FS run, add this line to find it
                 # -B ${LOCAL_FREESURFER_DIR}:/fsdir \
# Remove IsRunning files from FreeSurfer
# find ${LOCAL_FREESURFER_DIR}/sub-$subject/ -name "*IsRunning*" -type f -delete

# Parse the participants.tsv file and extract one subject ID from the
# line corresponding to this SLURM task.
subject=$(
  sed -n -E "$((${SLURM_ARRAY_TASK_ID} + 1))s/^([0-9]+)[[:space:]]+([0-9]+)$/\1/p" \
  ${BIDS_DIR}/code/${sublist}
)

# Compose the command line
cmd="module load Singularity/3.5.3-GCC-8.3.0 &&  \
     ${SINGULARITY_CMD} \
     /base \
     /base/${OUTPUT_DIR} \
     participant \
      --participant-label $subject \
      -w /work/ -vv \
      --fs-license-file /base/.license \
      --omp-nthreads 20 --nthreads 20 --mem_mb 80000 \
      --skip-bids-validation \
      --fs-subjects-dir /base/derivatives/freesurfer \
      --stop-on-first-crash \
      --bids-filter-file /base/code/bids_filter.json \
      --output-spaces T1w func MNI152NLin2009cAsym fsnative fsaverage "

# Add these two lines if you had freesurfer run already
#  --bids-filter-file /base/code/bids_filter_okazaki.json \
#  --use-syn-sdc \
#  --fs-subjects-dir /base/derivatives/fmriprep/analysis-okazaki_correctfmap/sourcedata/freesurfer
# --project-goodvoxels --notrack --mem_mb 60000 --nprocs 16 --omp-nthreads 8 --slice-time-ref 0
#      --fs-subjects-dir /fsdir"
#     --slice-time-ref 0 \
#     --project-goodvoxels \
#     --notrack \
# Setup done, run the command
echo Running task ${SLURM_ARRAY_TASK_ID}, for subject ${subject}
echo Commandline: $cmd
eval $cmd

echo "Coping the log files to the $LOG_DIR"
rsync -av $slurm_log_dir $LOG_DIR
exitcode=$?

# Output results to a table
echo "sub-$subject   ${SLURM_ARRAY_TASK_ID} $exitcode" >> ${LOG_DIR}/${SLURM_JOB_NAME}_${SLURM_ARRAY_JOB_ID}.tsv
echo Finished tasks ${SLURM_ARRAY_TASK_ID} with exit code $exitcode
exit $exitcode
