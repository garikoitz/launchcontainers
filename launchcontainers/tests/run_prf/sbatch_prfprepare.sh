#!/usr/bin/env bash
# MIT License
# Copyright (c) 2024-2025 Yongning Lei

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
baseP="/scratch/tlei/VOTCLOC"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

HOME_DIR="$baseP/singularity_home"
license_path="$baseP/BIDS/.license"

step="prfprepare"
version="1.5.0"
qos="regular"         # regular | test
mem="12G"
cpus="2"
time="00:30:00"
task="all"

json_dir="$baseP/code/${step}_jsons"
sif_path="/scratch/tlei/containers/${step}_${version}.sif"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
usage() {
    echo "Usage:"
    echo "  $0 -s <sub>,<ses>               # single sub/ses pair, e.g. -s 03,01"
    echo "  $0 -f <full_path_to_subseslist> # batch from file"
    exit 1
}

subses_arg=""
file_arg=""

while getopts ":s:f:" opt; do
    case $opt in
        s) subses_arg="$OPTARG" ;;
        f) file_arg="$OPTARG"   ;;
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
    analysis_name="sub$(echo "$subses_arg" | cut -d',' -f1)ses$(echo "$subses_arg" | cut -d',' -f2)"
else
    subseslist_path="$file_arg"
    if [[ ! -f "$subseslist_path" ]]; then
        echo "Error: subseslist not found: $subseslist_path"
        exit 1
    fi
    tail -n +2 "$subseslist_path" > "$tmpfile"
    analysis_name=$(basename "$file_arg" | sed 's/\.[^.]*$//')
fi

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
LOG_DIR="$baseP/dipc_${step}_logs/$(date +"%Y-%m-%d")_${analysis_name}"
mkdir -p "$LOG_DIR"
mkdir -p "$HOME_DIR"

cp "$0" "$LOG_DIR/"
[[ -n "$file_arg" ]] && cp "$subseslist_path" "$LOG_DIR/subseslist.txt"

echo "Log dir  : $LOG_DIR"
echo "JSON dir : $json_dir"
echo ""

# ---------------------------------------------------------------------------
# Submit SLURM jobs
# ---------------------------------------------------------------------------
job_num=1
while IFS=',' read -r sub ses _; do
    [[ -z "$sub" || -z "$ses" ]] && continue

    json_path="${json_dir}/${task}_sub-${sub}_ses-${ses}.json"
    if [[ ! -f "$json_path" ]]; then
        echo "WARNING: JSON not found for sub-${sub} ses-${ses}: $json_path — skipping"
        continue
    fi

    now=$(date +"%H-%M")
    cmd="sbatch -J ${job_num}_${task}_${step} \
        --time=${time} \
        -n 1 \
        --cpus-per-task=${cpus} \
        --mem=${mem} \
        --partition=general \
        --qos=${qos} \
        -o ${LOG_DIR}/${now}_%J_%x_${sub}-${ses}.o \
        -e ${LOG_DIR}/${now}_%J_%x_${sub}-${ses}.e \
        --export=ALL,baseP=${baseP},license_path=${license_path},version=${version},sub=${sub},ses=${ses},json_path=${json_path},sif_path=${sif_path} \
        ${script_dir}/${step}_dipc.sh"

    echo "Submitting: sub-${sub} ses-${ses}"
    eval "$cmd"

    ((job_num++))
done < "$tmpfile"

rm -f "$tmpfile"
