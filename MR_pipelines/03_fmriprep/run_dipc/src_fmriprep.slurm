#!/bin/bash
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

#SBATCH --time=18:00:00
#SBATCH -n 1  # this stands for number of task/ or number of nodes being used
#SBATCH --cpus-per-task=30    #6
#SBATCH --mem=65000
#SBATCH --partition=general  # Queue names you can submit to
#SBATCH --qos=regular       #<regular,serial,test,long or xlong>

# if you want to syn the logs to the analysis folder,
# need to create the log folder first
# the sublist is shared across launch.sh and the slurm

echo "output_base is ${output_base}"
echo "bids_dir is ${bids_dir}"

# Read-only BIDS input (on /data — not writable)
BIDS_DIR="${bids_dir}"
# Scratch-side paths (all writable)
CODE_DIR="${output_base}/code"
fp_output_dir="${output_base}/derivatives_new/fmriprep-${fp_version}_${analysis_name}"
# Prepare output folder (on scratch — writable)
mkdir -p ${fp_output_dir}

LOG_DIR="${fp_output_dir}/logs"
echo "fp_output_dir: $fp_output_dir"
mkdir -p ${LOG_DIR}

export cache_dir=/scratch/tlei/fmriprep_tmps_${fp_version}_${analysis_name}

# Prepare some writeable bind-mount points.
TEMPLATEFLOW_HOST_HOME=$cache_dir/.cache/templateflow
FMRIPREP_HOST_CACHE=$cache_dir/.cache/fmriprep
FMRIPREP_WORK_DIR=$cache_dir/.work/fmriprep
mkdir -p ${TEMPLATEFLOW_HOST_HOME}
mkdir -p ${FMRIPREP_HOST_CACHE}
mkdir -p ${FMRIPREP_WORK_DIR}


# This trick will help you reuse freesurfer results across pipelines and fMRIPrep versions
# mkdir -p ${BIDS_DIR}/derivatives/freesurfer-6.0.1
# if [ ! -d ${BIDS_DIR}/${fp_output_dir}/freesurfer ]; then
#         ln -s ${BIDS_DIR}/derivatives/freesurfer-6.0.1
#         ${BIDS_DIR}/${fp_output_dir}/freesurfer
#         fi

# Make sure FS_LICENSE is defined in the container.
# License file lives in the (read-only) BIDS dir — reading it is fine.
export SINGULARITYENV_FS_LICENSE=${BIDS_DIR}/.license
# Designate a templateflow bind-mount point
export SINGULARITYENV_TEMPLATEFLOW_HOME="/templateflow"
# BIDS input  → /bids  (read-only bind so fmriprep cannot write there)
# Output dir  → /output (writable scratch)
# Work/cache  → /work  (writable scratch)
FS_DIR="/scratch/tlei/VOTCLOC/BIDS/derivatives/freesurfer-with_t2"
SINGULARITY_CMD="unset PYTHONPATH && export PYTHONDONTWRITEBYTECODE=1 && singularity run --cleanenv --no-home --writable-tmpfs\
                 -B ${BIDS_DIR}:/bids \
                 -B ${fp_output_dir}:/output \
                 -B $CODE_DIR:/code \
                 -B ${TEMPLATEFLOW_HOST_HOME}:${SINGULARITYENV_TEMPLATEFLOW_HOME}\
                 -B ${FMRIPREP_HOST_CACHE}:/work \
                 -B ${FS_DIR}:/fsdir \
                 /scratch/tlei/containers/fmriprep_${fp_version}.sif "

                 # If you already have FS run, add this line to find it
                 # -B ${LOCAL_FREESURFER_DIR}:/fsdir \
# Remove IsRunning files from FreeSurfer
# find ${LOCAL_FREESURFER_DIR}/sub-$subject/ -name "*IsRunning*" -type f -delete

# Parse the participants.tsv file and extract one subject ID from the
# line corresponding to this SLURM task.
# this is getting the sub from \t operator
# subject=$(
#   sed -n -E "$((${SLURM_ARRAY_TASK_ID} + 1))s/^([0-9]+)[[:space:]]+([0-9]+)$/\1/p" \
#   ${output_base}/code/${sublist}
# )
# this is get the sub from , operator
subject=$(
  sed -n -E "$((${SLURM_ARRAY_TASK_ID} + 1))s/^([^,]+),.*$/\1/p" \
    "${sublist}"
)
ses=$(
  sed -n -E "$((${SLURM_ARRAY_TASK_ID} + 1))s/^[^,]+,([^,]+),.*$/\1/p" \
    "${sublist}"
)
scontrol update JobId=${SLURM_JOB_ID} Name="fp_s${subject}_${ses}"
# Compose the command line
cmd_anatonly="module load Apptainer/1.3.5 &&  \
     ${SINGULARITY_CMD} \
     /bids \
     /output \
     participant \
      --participant-label $subject \
      -w /work/ -vv \
      --fs-license-file /bids/.license \
      --omp-nthreads 10 --nthreads 20 --mem_mb 50000 \
      --skip-bids-validation \
      --anat-only \
      --stop-on-first-crash \
      --bids-filter-file /code/fmriprep/bids_filter_sub-${subject}.json \
      --output-spaces T1w MNI152NLin2009cAsym fsnative fsaverage "

cmd_runall="module load Apptainer/1.3.5 &&  \
     ${SINGULARITY_CMD} \
     /bids \
     /output \
     participant \
      --participant-label $subject \
      -w /work/ -vv \
      --fs-license-file /bids/.license \
      --omp-nthreads 10 --nthreads 20 --mem_mb 50000 \
      --skip-bids-validation \
      --fs-subjects-dir /fsdir \
      --force bbr \
      --bold2anat-init t2w \
      --output-spaces T1w func MNI152NLin2009cAsym fsnative fsaverage "

# Add these two lines if you had freesurfer run already
#             --force bbr \
#       --dummy-scans 5 \
#       --bids-filter-file /code/fmriprep/bids_filter_sub-${subject}.json \
#       --fs-subjects-dir /base/derivatives/freesurfer \
#  --bids-filter-file /base/bids_filter.json \
#  --use-syn-sdc \
#  --fs-subjects-dir /base/derivatives/fmriprep/analysis-okazaki_correctfmap/sourcedata/freesurfer
# --project-goodvoxels --notrack --mem_mb 60000 --nprocs 16 --omp-nthreads 8 --slice-time-ref 0
#      --fs-subjects-dir /fsdir"
#     --slice-time-ref 0 \
#     --project-goodvoxels \
#     --notrack \
# --stop-on-first-crash \ this is dangerous, since the rendering step can fail for some subjects, but the rest of the pipeline is fine. Use with caution.
# Pre-warm the TemplateFlow cache before running the main command.
# Multiple array tasks starting simultaneously on a fresh cache will race
# on templateflow registry initialization, causing some tasks to see only
# partially-populated template lists (e.g. missing MNI152NLin2009cAsym).
# Task 1 runs fmriprep --version (triggers tf registry init) while others wait.
if [ "${SLURM_ARRAY_TASK_ID}" -eq 1 ]; then
    echo "Task 1: pre-warming TemplateFlow cache..."
    module load Apptainer/1.3.5
    eval "${SINGULARITY_CMD} --version"
    echo "TemplateFlow cache pre-warm done."
    touch "${FMRIPREP_WORK_DIR}/.tf_cache_ready"
else
    echo "Task ${SLURM_ARRAY_TASK_ID}: waiting for TemplateFlow cache pre-warm..."
    for i in $(seq 1 60); do
        [ -f "${FMRIPREP_WORK_DIR}/.tf_cache_ready" ] && break
        sleep 5
    done
    echo "Task ${SLURM_ARRAY_TASK_ID}: proceeding."
fi

# Setup done, run the command
echo Running task ${SLURM_ARRAY_TASK_ID}, for subject ${subject}
echo Commandline: $cmd_runall
eval $cmd_runall
exitcode=$?

echo "Coping the log files to the $LOG_DIR"
rsync -av $slurm_log_dir $LOG_DIR

# Output results to a table
echo "sub-$subject   ${SLURM_ARRAY_TASK_ID} $exitcode" >> ${LOG_DIR}/${SLURM_JOB_NAME}_${SLURM_ARRAY_JOB_ID}.tsv
echo Finished tasks ${SLURM_ARRAY_TASK_ID} with exit code $exitcode
exit $exitcode
