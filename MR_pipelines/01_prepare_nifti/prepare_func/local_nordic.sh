#!/usr/bin/env bash

# Configuration /export/home/tlei/tlei/soft/launchcontainers/MR_pipelines/01_prepare_nifti/prepare_func
script_dir=/export/home/tlei/tlei/soft/launchcontainers/MR_pipelines/01_prepare_nifti/prepare_func
analysis_name='sub110102'
# variable pass to the matlab
TB_PATH="/export/home/tlei/tlei/toolboxes"
SRC_DIR="/bcbl/home/public/Gari/VOTCLOC/main_exp/raw_nifti"
OUTPUT_DIR="/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS"
codedir=/bcbl/home/public/Gari/VOTCLOC/main_exp/code
subseslist_name=$1
subseslist_path=$codedir/$subseslist_name

# nordic parameters (must match your MATLAB signature)
NORDIC_END=1
FORCE=false
DONORDIC=true
DOTSNR=false

# Ensure output dir exists
mkdir -p "${OUTPUT_DIR}"


# define the log dir
logdir=${OUTPUT_DIR}/log_nordic_fmri/${analysis_name}_$(date +"%Y-%m-%d")
echo "The logdir is $logdir"
echo "The outputdir is $OUTPUT_DIR"
mkdir -p $logdir
# Loop through sub/ses list and invoke single-job script
while IFS=',' read -r sub ses
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
    cmd="bash $script_dir/src_nordic_fmri.sh ${TB_PATH} \
    ${SRC_DIR} \
    ${OUTPUT_DIR} \
    ${sub} \
    ${ses} \
    ${NORDIC_END} \
    ${DONORDIC} \
    ${DOTSNR} \
    ${FORCE} \
    ${script_dir} "
    eval $cmd  > ${log_file} 2> ${error_file}
done < "${subseslist_path}"
