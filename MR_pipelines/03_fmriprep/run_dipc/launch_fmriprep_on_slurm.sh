#!/usr/bin/env bash
# MIT License
# Copyright (c) 2024-2025 Yongning Lei

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
OUTPUT_BASE="/scratch/tlei/VOTCLOC"       # scratch: code, logs, output
BIDS_DIR="/data/tlei/VOTCLOC/BIDS_new"  # read-only data source

script_dir="/scratch/tlei/soft/launchcontainers/MR_pipelines/03_fmriprep"

fp_version=25.1.4

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
usage() {
    echo "Usage:"
    echo "  $0 -a <analysis_name> -s <sub>,<ses>         # single sub/ses pair"
    echo "  $0 -a <analysis_name> -f <subseslist_name>   # batch from codedir/<subseslist_name>"
    echo ""
    echo "## Note: sublist is subject-only; all sessions are processed together."
    echo "## To target specific sessions use a bids_filter.json in the analysis config."
    exit 1
}

subses_arg=""
file_arg=""
analysis_name=""

while getopts ":a:s:f:" opt; do
    case $opt in
        a) analysis_name="$OPTARG" ;;
        s) subses_arg="$OPTARG" ;;
        f) file_arg="$OPTARG" ;;
        *) usage ;;
    esac
done

if [[ -z "$analysis_name" ]]; then
    echo "Error: -a <analysis_name> is required"
    usage
fi

if [[ -z "$subses_arg" && -z "$file_arg" ]]; then
    usage
fi

# ---------------------------------------------------------------------------
# Logging setup (must happen before building sublist — sublist lives in logdir)
# ---------------------------------------------------------------------------
slurm_log_dir=$OUTPUT_BASE/dipc_fmriprep/${fp_version}_${analysis_name}_$(date +"%Y-%m-%d")
mkdir -p "${slurm_log_dir}"

cp "$0" "${slurm_log_dir}/"

# ---------------------------------------------------------------------------
# Build persistent sublist in logdir (SLURM workers read it asynchronously)
# ---------------------------------------------------------------------------
sublist="${slurm_log_dir}/subseslist.txt"

if [[ -n "$subses_arg" ]]; then
    job_name="sub$(echo "$subses_arg" | cut -d',' -f1)ses$(echo "$subses_arg" | cut -d',' -f2)"
    printf "sub,ses\n%s\n" "$subses_arg" > "$sublist"
else
    src_sublist="$file_arg"
    if [[ ! -f "$src_sublist" ]]; then
        echo "Error: subseslist not found: $src_sublist"
        exit 1
    fi
    cp "$src_sublist" "$sublist"
    first_sub=$(awk -F',' 'NR==2{print $1}' "$sublist")
    first_ses=$(awk -F',' 'NR==2{print $2}' "$sublist")
    job_name="fp_s${first_sub}_${first_ses}"
fi

# ---------------------------------------------------------------------------
# Submit SLURM array
# ---------------------------------------------------------------------------
TOTAL_LINES=$(wc -l < "$sublist")
DATA_LINES=$((TOTAL_LINES - 1))

echo ""
echo "========================================"
echo "  fMRIPrep SLURM submission"
echo "========================================"
echo "  analysis    : ${analysis_name}"
echo "  fp_version  : ${fp_version}"
echo "  bids_dir    : ${BIDS_DIR}"
echo "  output_base : ${OUTPUT_BASE}"
echo "  log_dir     : ${slurm_log_dir}"
echo "  sublist     : ${sublist}"
echo "  n_jobs      : ${DATA_LINES}"
echo "----------------------------------------"
echo "  subjects:"
awk -F',' 'NR>1 {printf "    [%d] sub-%s  ses-%s\n", NR-1, $1, $2}' "$sublist"
echo "========================================"
echo ""

export analysis_name fp_version sublist slurm_log_dir BIDS_DIR

now=$(date +"%H-%M")

cmd="sbatch \
    --export=analysis_name=${analysis_name},fp_version=${fp_version},slurm_log_dir=${slurm_log_dir},sublist=${sublist},output_base=${OUTPUT_BASE},bids_dir=${BIDS_DIR} \
    --array=1-${DATA_LINES} \
    -J ${job_name} \
    -o ${slurm_log_dir}/%J_%x-%A-%a_${now}.o \
    -e ${slurm_log_dir}/%J_%x-%A-%a_${now}.e \
    ${script_dir}/run_dipc/src_fmriprep.slurm"

echo "sbatch cmd: $cmd"
echo ""
eval "$cmd"
