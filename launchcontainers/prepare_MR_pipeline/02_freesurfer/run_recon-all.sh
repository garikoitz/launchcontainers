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
# BIDS ID
sub=$1
ses=$2

surfdir="/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS/derivatives/freesurfer"

export SUBJECTS_DIR=${surfdir}

###########################################################
###########################################################
###########################################################
module load freesurfer/7.3.2

basedir=/bcbl/home/public/Gari/VOTCLOC/main_exp
bids_dirname=BIDS
T1_path="$basedir/$bids_dirname/sub-$sub/ses-$ses/anat/sub-${sub}_ses-${ses}_run-01_T1w.nii.gz"
outputdir=${basedir}/${bids_dirname}
step=reconall
analysis_name=check0709
logdir=${outputdir}/log_${step}/${analysis_name}_$(date +"%Y-%m-%d")
echo "The logdir is $logdir"
echo "The outputdir is $outputdir"
mkdir -p $logdir

now=$(date +"%H;%M")
log_file="${logdir}/reconall_${sub}_${ses}_${now}.o"
error_file="${logdir}/reconall_${sub}_${ses}_${now}.e"

cmd="recon-all -i ${T1_path} \
          -subjid sub-${sub} \
          -sd ${basedir}/${bids_dirname}/derivatives/freesurfer \
          -all "

echo "Going to run recon-all on sub-${sub}"
echo $cmd
eval $cmd > ${log_file} 2> ${error_file}
