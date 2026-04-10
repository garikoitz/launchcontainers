#!/usr/bin/env bash
# MIT License
# Copyright (c) 2024-2025 Yongning Lei

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
baseP="/scratch/tlei/VOTCLOC"
codedir="$baseP/code"
script_dir="/home/tlei/soft/launchcontainers/MR_pipelines/04_fMRI_ret"

HOME_DIR="$baseP/singularity_home"
license_path="$baseP/BIDS/.license"
model="css"           # "one_gaussian" or "css"

step="prfanalyze-vista"
version='2.2.1'
qos="regular"         # regular or test
mem="16G"
cpus="25"
time="8:00:00"
task="retFF"          # retCB retRW retFF retfixRW retfixFF

json_dir="$baseP/code/${step}_jsons"
sif_path="/scratch/tlei/containers/${step}_${version}.sif"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
usage() {
    echo "Usage:"
    echo "  $0 -s <sub>,<ses>         # single sub/ses pair"
    echo "  $0 -f <subseslist_name>   # batch from codedir/<subseslist_name>"
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
# Build sub/ses list
# ---------------------------------------------------------------------------
tmpfile=$(mktemp)

if [[ -n "$subses_arg" ]]; then
    echo "$subses_arg" > "$tmpfile"
    log_note="sub$(echo "$subses_arg" | cut -d',' -f1)ses$(echo "$subses_arg" | cut -d',' -f2)"
else
    subseslist_path="$codedir/$file_arg"
    if [[ ! -f "$subseslist_path" ]]; then
        echo "Error: subseslist not found: $subseslist_path"
        exit 1
    fi
    tail -n +2 "$subseslist_path" > "$tmpfile"
    log_note=$(basename "$file_arg" | sed 's/\.[^.]*$//')
fi

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
LOG_DIR="/scratch/tlei/dipc_${step}_logs/$(date +"%Y-%m-%d")_${log_note}"
mkdir -p "$LOG_DIR"
mkdir -p "$HOME_DIR"

cp "$0" "$LOG_DIR/"
[[ -n "$file_arg" ]] && cp "$subseslist_path" "$LOG_DIR/subseslist.txt"

# ---------------------------------------------------------------------------
# Submit SLURM jobs
# ---------------------------------------------------------------------------
job_num=1
while IFS=',' read -r sub ses; do
    [[ -z "$sub" || -z "$ses" ]] && continue

    now=$(date +"%H-%M")
    cmd="sbatch -J ${job_num}_${task}_${step} \
        --time=${time} \
        -n 1 \
        --cpus-per-task=${cpus} \
        --mem=${mem} \
        --partition=general \
        --qos=${qos} \
        -o ${LOG_DIR}/%J_%x_${sub}-${ses}_${now}.o \
        -e ${LOG_DIR}/%J_%x_${sub}-${ses}_${now}.e \
        --export=ALL,baseP=${baseP},license_path=${license_path},version=${version},sub=${sub},ses=${ses},json_path=${json_dir}/${task}_${model}_sub-${sub}_ses-${ses}.json,sif_path=${sif_path} \
        ${script_dir}/run_dipc/${step}_dipc.sh"

    echo "Submitting job for sub-${sub} ses-${ses}"
    echo "$cmd"
    eval "$cmd"

    ((job_num++))
done < "$tmpfile"

rm -f "$tmpfile"
