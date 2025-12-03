
# """
# MIT License

# Copyright (c) 2024-2025 Yongning Lei

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial
# portions of the Software.
unset analysis_name
unset fp_version
unset sublist
unset slurm_log_dir

STUDY="/scratch/tlei/VOTCLOC"
code_dir="/scratch/tlei/soft/launchcontainers/MR_pipelines/03_fmriprep"
# the subseslist here is just a subseslist name, it will look for
# subseslist under basedir/code/subseslist
sublist_name=$1
sublist="${STUDY}/code/$sublist_name"
analysis_name='t2-fs_dummyscans-5_bold2anat-t2w_forcebbr'
fp_version=25.1.4
job_name=sub0406


slurm_log_dir=$STUDY/dipc_fmriprep/${fp_version}_${analysis_name}_$(date +"%Y-%m-%d")
mkdir -p ${slurm_log_dir}

export analysis_name
export fp_version
export sublist
export slurm_log_dir

TOTAL_LINES=$(wc -l < "$sublist")

echo "Total lines is $TOTAL_LINES"

DATA_LINES=$((TOTAL_LINES - 1))

now=$(date +"%H-%M")

cmd="sbatch \
    --export=ALL,analysis_name="${analysis_name}",fp_version="${fp_version}",slurm_log_dir="${slurm_log_dir}",sublist="${sublist}",basedir=${STUDY}\
    --array=1-${DATA_LINES} \
    -J "${job_name}"
    -o "$slurm_log_dir/%J_%x-%A-%a_${now}.o" \
    -e "$slurm_log_dir/%J_%x-%A-%a_${now}.e" \
    ${code_dir}/run_dipc/src_fmriprep.slurm "

## Attention! the sublist needs to be subject only, and all the session will be processed in the same time
## if want to specify session, then specify the bids_filter.json file,
echo " The command for the slurm is "
echo $cmd
eval $cmd
