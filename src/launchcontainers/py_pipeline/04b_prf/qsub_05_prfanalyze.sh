# -----------------------------------------------------------------------------
# Copyright (c) Yongning Lei 2024
# All rights reserved.
#
# This script is distributed under the Apache-2.0 license.
# You may use, distribute, and modify this code under the terms of the Apache-2.0 license.
# See the LICENSE file for details.
#
# THIS SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE, AND NONINFRINGEMENT.
#
# Author: Yongning Lei
# Email: yl4874@nyu.edu
# GitHub: https://github.com/yongninglei
# -----------------------------------------------------------------------------

basedir=/bcbl/home/public/Gari/VOTCLOC/main_exp
dicom_dirname=dicom
outputdir=$basedir/raw_nifti

codedir=$basedir/code/00_dicom_to_nifti
subseslist_path=$codedir/subseslist_${project}.txt
heuristicfile=$codedir/heuristic/heuristic_${project}.py
sing_path=/bcbl/home/public/Gari/singularity_images

analysis_name=analysis-afterDec09
logdir=${outputdir}/heudiconv_ips_log/$analysis_name/${step}_${project}
echo "The logdir is $logdir"
echo "The outputdir is $outputdir"
mkdir -p $logdir

cp "$0" "$logdir/qsub_heudiconv${step}_${project}"
cp "$codedir/src_heudiconv_${step}_${project}.sh" "$logdir"



cmd="qsub -q short.q \
	-S /bin/bash
	-M t.lei@bcbl.eu
	-N heudiconv_s-${sub}_s-${ses} \
	-o $logdir/heudiconv_sub-${sub}_ses-${ses}.o \
	-e $logdir/heudiconv_sub-${sub}_ses-${ses}.e \
	-l mem_free=16G \
	-v basedir=${basedir} \
	-v logdir=${logdir} \
	-v dicom_dirname=$dicom_dirname \
	-v outputdir=${outputdir} \
	-v sub=${sub} \
	-v ses=${ses} \
	-v heuristicfile=$heuristicfile \
	-v sing_path=$sing_path \
	$codedir/src_heudiconv_${step}_${project}.sh "

now=$(date +"%Y-%m-%dT%H:%M")
log_file="${logdir}/heudiconv_${sub}_${ses}_${step}_${now}.log"
error_file="${logdir}/heudiconv_${sub}_${ses}_${step}_${now}.err"
echo $cmd
eval $cmd > ${log_file} 2> ${error_file}
