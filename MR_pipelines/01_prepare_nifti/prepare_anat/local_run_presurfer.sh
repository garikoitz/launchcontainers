#!/usr/bin/env bash
# MIT License
# Copyright (c) 2024-2025 Yongning Lei

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
step=presurfer
basedir=/bcbl/home/public/Gari/VOTCLOC/main_exp
codedir=/bcbl/home/public/Gari/VOTCLOC/main_exp/code

tbPath=/export/home/tlei/tlei/toolboxes
src_dir=$basedir/raw_nifti
outputdir=${basedir}/BIDS
force=false
script_dir=/export/home/tlei/tlei/soft/launchcontainers/MR_pipelines/01_prepare_nifti/prepare_anat

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
usage() {
    echo "Usage:"
    echo "  $0 -s <sub>,<ses>         # single sub/ses pair"
    echo "  $0 -f <full_path_to_subseslist>   # batch mode"
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
logdir=${outputdir}/log_${step}/${analysis_name}_$(date +"%Y-%m-%d")
echo "The logdir is $logdir"
echo "The outputdir is $outputdir"
mkdir -p "$logdir"

cp "$0" "$logdir"
[[ -n "$file_arg" ]] && cp "$subseslist_path" "$logdir/subseslist.txt"

# ---------------------------------------------------------------------------
# Run jobs locally
# ---------------------------------------------------------------------------
while IFS=',' read -r sub ses _; do
    [[ -z "$sub" || -z "$ses" ]] && continue

    echo "### PRESURFER: sub-${sub} ses-${ses} ###"
    now=$(date +"%H;%M")
    log_file="${logdir}/presurfer_${sub}_${ses}_${now}.o"
    error_file="${logdir}/presurfer_${sub}_${ses}_${now}.e"

    export tbPath src_dir outputdir sub ses force script_dir

    cmd="bash $script_dir/src_${step}.sh"
    echo "$cmd"
    eval "$cmd" > "${log_file}" 2> "${error_file}"

done < "$tmpfile"

rm -f "$tmpfile"
