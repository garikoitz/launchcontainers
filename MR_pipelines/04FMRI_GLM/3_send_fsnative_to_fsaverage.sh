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


# okazaki fmriprep: analysis-okazaki_ST05
# tamagawa and BCBL magnitude fmriprep: analysis-BCBL2_magn_fmap_nochangedtype"

#proba maps
subs="01 02 03 04 05" #"01 02 ,03, 04, 05"
threshold=4
contrasts="RWvsCB RWvsCS RWvsFF RWvsSD AllvsNull PERvsNull LEXvsNull PERvsLEX WordvsLEX WordvsPER
 WordvsNull
 WordvsLEXPER
 WordvsAllnoWordnoLEX
 WordvsAllnoWord
 LEXvsAllnoLEXnoRW
 PERvsAllnoLEXnoRW
 SDvsCB
 CSvsFF
 FacesvsNull
 FacesvsLEX
 FacesvsPER
 FacesvsLEXPER
 FacesvsAllnoFace
 AdultvsChild
 LimbsvsNull
 LimbsvsLEX
 LimbsvsPER
 LimbsvsLEXPER
 LimbsvsAllnoLimbs
 BodysLimb "
sm='00'

surfdir=/bcbl/home/public/Gari/VOTCLOC/derivatives/fmriprep/analysis-okazaki_ST05/sourcedata/freesurfer


##########################################
export SUBJECTS_DIR=${surfdir}

module load freesurfer/7.3.2

for sub in ${subs}; do
for contrast in ${contrasts}; do
	# before it is "${surface_tmap_dir}/sub-$sub/ses-$ses/func/sub-${sub}_ses-${ses}_task-${task}_run-${run}_hemi-L_space-fsnative_desc-smoothed0${sm}_bold.func.gii"
	# there is one level /func/, I don't know why
	pmap_dir=/bcbl/home/public/Gari/VOTCLOC/VSS/derivatives/vertexwise_count/tmap/sub-${sub}

	pmap_fname=$pmap_dir/threshold${threshold}_${contrast}_fsnative_sm${sm}.gii
	pmap_fname_fsaverage=$pmap_dir/threshold${threshold}_${contrast}_fsaverage_sm${sm}.gii
	cmd="mri_surf2surf --srcsubject sub-${sub} --srcsurfval ${pmap_fname} --trgsubject fsaverage --trgsurfval ${pmap_fname_fsaverage} --hemi lh"

	echo $cmd
	eval $cmd
done
done


#"mris_fwhm --i ${gii_in} --o ${gii_out} --so --fwhm ${sm} --subject sub-${sub} --hemi lh"
