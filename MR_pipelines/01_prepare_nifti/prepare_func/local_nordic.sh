#!/usr/bin/env bash

# Configuration
script_dir=/export/home/tlei/tlei/soft/launchcontainers/MR_pipelines/01_prepare_nifti/prepare_func
TB_PATH="/export/home/tlei/tlei/toolboxes"
SRC_DIR="/bcbl/home/public/Gari/VOTCLOC/main_exp/raw_nifti"
OUTPUT_DIR="/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS"
codedir=/bcbl/home/public/Gari/VOTCLOC/main_exp/code

# nordic parameters (must match your MATLAB signature)
NORDIC_END=1
FORCE=true
DONORDIC=true
DOTSNR=false

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
usage() {
    echo "Usage:"
    echo "  $0 -s <sub>,<ses>          # run a single sub/ses pair"
    echo "  $0 -f <full_path_to_subseslist>    # batch mode"
    exit 1
}

subses_arg=""
file_arg=""

while getopts ":s:f:" opt; do
    case $opt in
        s) subses_arg="$OPTARG" ;;
        f) file_arg="$OPTARG" ;;
        *) usage ;;
    esac
done

if [[ -z "$subses_arg" && -z "$file_arg" ]]; then
    usage
fi

# Build a temporary list of "sub,ses" lines to process
tmpfile=$(mktemp)

if [[ -n "$subses_arg" ]]; then
    # Single pair: expect "10,02"
    echo "$subses_arg" > "$tmpfile"
else
    # File mode: skip header line (first line), read the rest
    subseslist_path="$file_arg"
    if [[ ! -f "$subseslist_path" ]]; then
        echo "Error: subseslist not found: $subseslist_path"
        exit 1
    fi
    tail -n +2 "$subseslist_path" > "$tmpfile"
fi

# ---------------------------------------------------------------------------
# Determine analysis_name for log dir
# ---------------------------------------------------------------------------
if [[ -n "$subses_arg" ]]; then
    sub_tmp=$(echo "$subses_arg" | cut -d',' -f1)
    ses_tmp=$(echo "$subses_arg" | cut -d',' -f2)
    analysis_name="sub${sub_tmp}ses${ses_tmp}"
else
    analysis_name=$(basename "$file_arg" | sed 's/\.[^.]*$//')
fi

# ---------------------------------------------------------------------------
# Ensure output dir and log dir exist
# ---------------------------------------------------------------------------
mkdir -p "${OUTPUT_DIR}"
logdir="${OUTPUT_DIR}/log_nordic_fmri/${analysis_name}_$(date +"%Y-%m-%d")"
echo "The logdir is $logdir"
echo "The outputdir is $OUTPUT_DIR"
mkdir -p "$logdir"

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
while IFS=',' read -r sub ses _; do
    # Skip empty lines
    [[ -z "$sub" || -z "$ses" ]] && continue

    now=$(date +"%H;%M")
    log_file="${logdir}/nordic_${sub}_${ses}_${now}.o"
    error_file="${logdir}/nordic_${sub}_${ses}_${now}.e"
    echo "=== Running sub-${sub} ses-${ses} ==="

    cmd="bash $script_dir/src_nordic_fmri.sh \
        ${TB_PATH} \
        ${SRC_DIR} \
        ${OUTPUT_DIR} \
        ${sub} \
        ${ses} \
        ${NORDIC_END} \
        ${DONORDIC} \
        ${DOTSNR} \
        ${FORCE} \
        ${script_dir}"
    eval $cmd > "${log_file}" 2> "${error_file}"
done < "$tmpfile"

rm -f "$tmpfile"
