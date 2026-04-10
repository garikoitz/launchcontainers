#!/usr/bin/env bash
# MIT License
# Copyright (c) 2024-2025 Yongning Lei

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
basedir=/bcbl/home/public/Gari/VOTCLOC/main_exp
codedir=/bcbl/home/public/Gari/VOTCLOC/main_exp/code
bids_dirname=BIDS
outputdir=${basedir}/${bids_dirname}
step=reconall

module load freesurfer/7.3.2
export SUBJECTS_DIR="${outputdir}/derivatives/freesurfer"

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
    analysis_name="sub$(echo "$subses_arg" | cut -d',' -f1)ses$(echo "$subses_arg" | cut -d',' -f2)"
else
    subseslist_path="$codedir/$file_arg"
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
logdir=${outputdir}/log_${step}/${analysis_name}_$(date +"%Y-%m-%d")
echo "The logdir is $logdir"
echo "The outputdir is $outputdir"
mkdir -p "$logdir"

cp "$0" "$logdir"
[[ -n "$file_arg" ]] && cp "$subseslist_path" "$logdir/subseslist.txt"

# ---------------------------------------------------------------------------
# Run recon-all
# ---------------------------------------------------------------------------
while IFS=',' read -r sub ses; do
    [[ -z "$sub" || -z "$ses" ]] && continue

    echo "### recon-all: sub-${sub} ses-${ses} ###"
    now=$(date +"%H;%M")
    log_file="${logdir}/reconall_${sub}_${ses}_${now}.o"
    error_file="${logdir}/reconall_${sub}_${ses}_${now}.e"

    T1_path="$basedir/$bids_dirname/sub-${sub}/ses-${ses}/anat/sub-${sub}_ses-${ses}_run-01_T1w.nii.gz"

    cmd="recon-all -i ${T1_path} \
        -subjid sub-${sub} \
        -sd ${outputdir}/derivatives/freesurfer \
        -all"

    echo "Going to run recon-all on sub-${sub} ses-${ses}"
    echo "$cmd"
    eval "$cmd" > "${log_file}" 2> "${error_file}"

done < "$tmpfile"

rm -f "$tmpfile"
