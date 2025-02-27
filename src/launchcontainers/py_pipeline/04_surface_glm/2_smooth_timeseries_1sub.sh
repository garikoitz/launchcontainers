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
#########################################
codedir=/bcbl/home/public/Gari/VOTCLOC/VSS/code/05_surface_glm


fmriprep_dir=/bcbl/home/public/Gari/VOTCLOC/VSS/BIDS/derivatives/fmriprep/analysis-rerun_nordic_fmap
surfdir=/bcbl/home/public/Gari/VOTCLOC/derivatives/fmriprep/analysis-okazaki_ST05/sourcedata/freesurfer


sub='05'
sess='day3PF day5BCBL day6BCBL'
runs="01 02 03 04 05 06"
smms="2"

task=fLoc
echo "the smoothing is going to start"
echo "current directory is $PWD"

space='fsnative'

##########################################
export SUBJECTS_DIR=${surfdir}

module load freesurfer/7.3.2

for ses in $sess; do
for run in $runs; do
for sm in ${smms}; do 
	gii_in="${fmriprep_dir}/sub-$sub/ses-$ses/func/sub-${sub}_ses-${ses}_task-${task}_run-${run}_hemi-L_space-${space}_bold.func.gii"
	gii_out="${fmriprep_dir}/sub-$sub/ses-$ses/func/sub-${sub}_ses-${ses}_task-${task}_run-${run}_hemi-L_space-${space}_desc-smoothed0${sm}_bold.func.gii"
		
	cmd="mris_fwhm --i ${gii_in} --o ${gii_out} --so --fwhm ${sm} --subject sub-${sub} --hemi lh"
	echo $cmd
	eval $cmd
done
done
done

