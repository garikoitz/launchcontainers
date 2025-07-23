# # MIT License

# Copyright (c) 2020-2023 Garikoitz Lerma-Usabiaga
# Copyright (c) 2022-2023 Yongning Lei
# Copyright (c) 2023 David Linhardt

# Permission is hereby granted, free of charge,
# to any person obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies
# or substantial portions of the Software.
basedir="/bcbl/home/public/Gari/VOTCLOC/main_exp"
analysis_name="sub10"
fp_version=25.1.3
CODE_DIR="${basedir}/code"
BIDS_DIR="${basedir}/BIDS"
OUTPUT_DIR=derivatives/fmriprep-${fp_version}_${analysis_name}
DERIVS_DIR="${BIDS_DIR}/$OUTPUT_DIR"

# if BIDS is BIDS. then cache_dir=$basedir
# otherwise, it is cache_dir=$BIDS_DIR
export cache_dir=$basedir/fmriprep_tmps_$analysis_name
LOG_DIR=$DERIVS_DIR/logs


#LOCAL_FREESURFER_DIR="/dipc/tlei/.license"

# Prepare some writeable bind-mount points.
TEMPLATEFLOW_HOST_HOME=$cache_dir/.cache/templateflow
FMRIPREP_HOST_CACHE=$cache_dir/.cache/fmriprep
FMRIPREP_WORK_DIR=$cache_dir/.work/fmriprep
mkdir -p ${TEMPLATEFLOW_HOST_HOME}
mkdir -p ${FMRIPREP_HOST_CACHE}
mkdir -p ${FMRIPREP_WORK_DIR}
mkdir -p ${LOG_DIR}
# Prepare derivatives folder
mkdir -p ${DERIVS_DIR}

# This trick will help you reuse freesurfer results across pipelines and fMRIPrep versions
# mkdir -p ${BIDS_DIR}/derivatives/freesurfer-6.0.1
# if [ ! -d ${BIDS_DIR}/${DERIVS_DIR}/freesurfer ]; then
#         ln -s ${BIDS_DIR}/derivatives/freesurfer-6.0.1
#         ${BIDS_DIR}/${DERIVS_DIR}/freesurfer
#         fi

# Make sure FS_LICENSE is defined in the container.
export SINGULARITYENV_FS_LICENSE=/export/home/tlei/tlei/linux_settings/license.txt

# Designate a templateflow bind-mount point
export SINGULARITYENV_TEMPLATEFLOW_HOME="/templateflow"
# SINGULARITY_CMD="unset PYTHONPATH && singularity run --cleanenv --home /scratch/glerma \
#                  -B /scratch/glerma:/scratch/glerma \
SINGULARITY_CMD="unset PYTHONPATH && singularity run --cleanenv --no-home \
                     --containall --writable-tmpfs \
                 -B /bcbl:/bcbl \
                 -B /export:/export \
                 -B $BIDS_DIR:/base \
                 -B $CODE_DIR:/code \
                 -B ${TEMPLATEFLOW_HOST_HOME}:${SINGULARITYENV_TEMPLATEFLOW_HOME}\
                 -B ${FMRIPREP_HOST_CACHE}:/work \
                 /bcbl/home/public/Gari/containers/fmriprep_${fp_version}.sif"

                 # If you already have FS run, add this line to find it
                 # -B ${LOCAL_FREESURFER_DIR}:/fsdir \
# Remove IsRunning files from FreeSurfer
# find ${LOCAL_FREESURFER_DIR}/sub-$subject/ -name "*IsRunning*" -type f -delete

subject=$1
# Compose the command line
now=$(date +"%Y-%m-%dT%H:%M")
cmd="module load apptainer/latest &&  \
     ${SINGULARITY_CMD} \
     /base \
     /base/${OUTPUT_DIR} \
     participant --participant-label $subject \
     -w /work/ -vv \
     --fs-license-file ${SINGULARITYENV_FS_LICENSE} \
     --omp-nthreads 10 --nthreads 30 --mem_mb 80000 \
     --skip-bids-validation \
     --output-spaces T1w func MNI152NLin2009cAsym fsnative fsaverage \
     --notrack \
     --stop-on-first-crash \

     > ${LOG_DIR}/${analysis_name}_sub-${subject}_${now}.o 2> ${LOG_DIR}/${analysis_name}_sub-${subject}_${now}.e "

#     --bids-filter-file /base/code/bids_filter.json \
     #--fs-subjects-dir /base/BIDS/derivatives/freesurfer/analysis-${analysis_name} "
# Add these two lines if you had freesurfer run already
#  --bids-filter-file /base/code/bids_filter_okazaki.json \
#  --use-syn-sdc \
# --project-goodvoxels --notrack --mem_mb 60000 --nprocs 16 --omp-nthreads 8 --slice-time-ref 0
#      --fs-subjects-dir /fsdir"
#     --slice-time-ref 0 \
#     --project-goodvoxels \
#     --notrack \
#     --dummy-scans 6 \
# --fs-subjects-dir /base/BIDS/derivatives/fmriprep/analysis-beforeFebST05/sourcedata/freesurfer \
# --fs-subjects-dir /base/derivatives/fmriprep/analysis-okazaki_correctfmap/sourcedata/freesurfer
# Setup done, run the command
echo Running subject ${subject}
echo Commandline: $cmd
eval $cmd
