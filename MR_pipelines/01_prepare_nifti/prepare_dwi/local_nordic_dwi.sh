#!/usr/bin/env bash

# Configuration /export/home/tlei/tlei/soft/launchcontainers/MR_pipelines/01_prepare_nifti/prepare_dwi
nordic_modality="dwi" # dwi or fmri
if [ "$nordic_modality" != "dwi" ] && [ "$nordic_modality" != "fmri" ]
then
    echo "The nordic modality should be dwi or fmri"
    exit 1
fi

if [ "$nordic_modality" == "dwi" ]
then
    script_dir=/export/home/tlei/tlei/soft/launchcontainers/MR_pipelines/01_prepare_nifti/prepare_dwi
else
    script_dir=/export/home/tlei/tlei/soft/launchcontainers/MR_pipelines/01_prepare_nifti/prepare_func
fi

analysis_name=$1
# variable pass to the matlab
TB_PATH="/export/home/tlei/tlei/toolboxes"
SRC_DIR="/bcbl/home/public/Gari/VOTCLOC/main_exp/raw_nifti"
OUTPUT_DIR="/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS"
codedir=/bcbl/home/public/Gari/VOTCLOC/main_exp/code
subseslist_name=$2
subseslist_path=$codedir/$subseslist_name

# nordic parameters (must match your MATLAB signature)
NORDIC_END=0
FORCE=true
DONORDIC=true

# Ensure output dir exists
mkdir -p "${OUTPUT_DIR}"
# define the log dir
logdir=${OUTPUT_DIR}/log_nordic_${nordic_modality}/${analysis_name}_$(date +"%Y-%m-%d")
echo "The logdir is $logdir"
echo "The outputdir is $OUTPUT_DIR"
mkdir -p $logdir
# copy the subseslist to the logdir for record
cp $subseslist_path $logdir
# Loop through sub/ses list and invoke single-job script
while IFS=',' read -r sub ses _
do
    echo "line number is $line_number sub is $sub ses is $ses"
    # Increment line counter
    ((line_number++))

    # Skip the first line which is the header
    if [ $line_number -eq 1 ]; then
        continue
    fi

    # Define the name of logs
    now=$(date +"%H;%M")
    log_file="${logdir}/nordic_${sub}_${ses}_${now}.o"
    error_file="${logdir}/nordic_${sub}_${ses}_${now}.e"
    echo "=== Running sub-${sub} ses-${ses} ==="
    #echo "script dir defined is $script_dir"
    cmd="bash $script_dir/src_nordic_${nordic_modality}.sh ${TB_PATH} \
    ${SRC_DIR} \
    ${OUTPUT_DIR} \
    ${sub} \
    ${ses} \
    ${NORDIC_END} \
    ${DONORDIC} \
    ${FORCE} \
    ${script_dir} "
    eval $cmd  > ${log_file} 2> ${error_file}
done < "${subseslist_path}"
