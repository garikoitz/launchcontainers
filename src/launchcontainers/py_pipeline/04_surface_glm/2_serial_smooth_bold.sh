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
subseslist_path=${codedir}/subseslist_sub05.tsv

# okazaki fmriprep: analysis-okazaki_ST05
# tamagawa and BCBL magnitude fmriprep: analysis-BCBL2_magn_fmap_nochangedtype"
fmriprep_dir=/bcbl/home/public/Gari/VOTCLOC/VSS/BIDS/derivatives/fmriprep/analysis-okazaki_ST05

surfdir=/bcbl/home/public/Gari/VOTCLOC/derivatives/fmriprep/analysis-okazaki_ST05/sourcedata/freesurfer

runs="01 02 03 04 05 06"
smms="2 "
task=fLoc
##########################################
export SUBJECTS_DIR=${surfdir}

module load freesurfer/7.3.2
# Initialize a line counter
line_number=0

# Read the file line by line
while IFS=$'\t' read -r sub ses
do
    # Increment line counter
    ((line_number++))

    # Skip the first line which is the header
    if [ $line_number -eq 1 ]; then
        continue
    fi
	
	echo "the smoothing is going to start"
	echo "current directory is $PWD"

	module load freesurfer/7.3.2
	for run in ${runs}; do
	for sm in ${smms}; do 
		gii_in="${fmriprep_dir}/sub-$sub/ses-$ses/func/sub-${sub}_ses-${ses}_task-${task}_run-${run}_hemi-L_space-fsaverage_bold.func.gii"
		gii_out="${fmriprep_dir}/sub-$sub/ses-$ses/func/sub-${sub}_ses-${ses}_task-${task}_run-${run}_hemi-L_space-fsaverage_desc-smoothed0${sm}_bold.func.gii"
		echo "woring on sub-${sub} ses-${ses}"	
		cmd="mris_fwhm --i ${gii_in} --o ${gii_out} --fwhm ${sm} --subject sub-${sub} --hemi lh"
		echo $cmd
		eval $cmd
done
done
done < "$subseslist_path"

#"mris_fwhm --i ${gii_in} --o ${gii_out} --so --fwhm ${sm} --subject sub-${sub} --hemi lh"