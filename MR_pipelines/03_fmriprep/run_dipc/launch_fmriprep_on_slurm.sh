#!/usr/bin/env bash
# MIT License
# Copyright (c) 2024-2025 Yongning Lei

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
STUDY="/scratch/tlei/VOTCLOC"
codedir="${STUDY}/code"
code_dir="/scratch/tlei/soft/launchcontainers/MR_pipelines/03_fmriprep"

analysis_name='t2-fs_dummyscans-5_bold2anat-t2w_forcebbr'
fp_version=25.1.4

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
usage() {
    echo "Usage:"
    echo "  $0 -s <sub>,<ses>         # single sub/ses pair"
    echo "  $0 -f <subseslist_name>   # batch from codedir/<subseslist_name>"
    echo ""
    echo "## Note: sublist is subject-only; all sessions are processed together."
    echo "## To target specific sessions use a bids_filter.json in the analysis config."
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

# ---------------------------------------------------------------------------
# Logging setup (must happen before building sublist — sublist lives in logdir)
# ---------------------------------------------------------------------------
slurm_log_dir=$STUDY/dipc_fmriprep/${fp_version}_${analysis_name}_$(date +"%Y-%m-%d")
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
    src_sublist="$codedir/$file_arg"
    if [[ ! -f "$src_sublist" ]]; then
        echo "Error: subseslist not found: $src_sublist"
        exit 1
    fi
    job_name=$(basename "$file_arg" | sed 's/\.[^.]*$//')
    cp "$src_sublist" "$sublist"
fi

# ---------------------------------------------------------------------------
# Submit SLURM array
# ---------------------------------------------------------------------------
TOTAL_LINES=$(wc -l < "$sublist")
DATA_LINES=$((TOTAL_LINES - 1))

echo "slurm_log_dir : $slurm_log_dir"
echo "sublist       : $sublist  ($DATA_LINES job(s))"

export analysis_name fp_version sublist slurm_log_dir

now=$(date +"%H-%M")

cmd="sbatch \
    --export=ALL,analysis_name=${analysis_name},fp_version=${fp_version},slurm_log_dir=${slurm_log_dir},sublist=${sublist},basedir=${STUDY} \
    --array=1-${DATA_LINES} \
    -J ${job_name} \
    -o ${slurm_log_dir}/%J_%x-%A-%a_${now}.o \
    -e ${slurm_log_dir}/%J_%x-%A-%a_${now}.e \
    ${code_dir}/run_dipc/src_fmriprep.slurm"

echo "The command for slurm is:"
echo "$cmd"
eval "$cmd"
