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
#subs="S006"
#subs="S001 S002 S003 S005 S006 S007 S008 S009 S010 "
subs="S001 S002 S003 S005 S006 S007 S008 S009 S010 S009 S010 S011 S012 S014 S015 S016 S017 S019\
      S020 S021 S022 S023 S024 S025 S026 S027 S028 \
      S030 S031 S033 S034 S035 S041 S042 S043 S044 \
      S045 S046 S047 S049 S050 S051 S052 S053 S054 \
      S055 S057 S058 S059 S060 S061 S062 S063 S064 \
      S065 S066 S068 S069 S070 S101"

surfdir="/bcbl/home/public/Gari/MINI/ANALYSIS/freesurferacpc"
designs="block event" 
outputdir="/bcbl/home/public/Gari/MINI/ANALYSIS/individual_subject_analysis/VOTmasked"
# degree of freedom is 297
# p=.05 ct=1.65
# p=.01 ct=2.35
# p=0.001 ct=3.13
p005=1.65
p001=2.35
p0001=3.13

stimuli1="RW Faces"
stimuli2="PW CS FF PS CB SD"


#LEX_PER_stimuli="CS FF PW CB PS SD"

module load freesurfer/7.3.2
###########################################################
###########################################################
###########################################################
cmd_display="-view inferior \
-viewport 3d \
-zoom 1 "

for design in ${designs}; do
overlaydir="/bcbl/home/public/Gari/MINI/ANALYSIS/fMRI_SPM/${design}/analysis_${design}_acpc_lhITfusLatOcc/SUBJECTS"
for sub in ${subs}; do
# generate the overlay command and concat it with surf cmd
sub_outputdir=${outputdir}"/${sub}/${design}"
	if [ ! -d ${sub_outputdir} ]; then
		mkdir -p ${sub_outputdir}
		echo "create sub folder for store the output ${sub_outputdir}"
	else 
		echo "folder already there"
fi

for stim1 in ${stimuli1}; do
	for stim2 in ${stimuli2}; do
		cmd_overlay=":overlay=${overlaydir}/${sub}/results/${stim1}vs${stim2}.mgh:overlay_threshold=${p005},${p0001}:visible=1"
		cmd_snapshot="-ss ${sub_outputdir}/${design}_threshold${p005}_${p0001}_${stim1}vs${stim2}.png -quit "		
		cmd_surf="freeview -f ${surfdir}/${sub}/surf/lh.inflated:curvature_method=binary:name=${sub}-inflated:visible=1:label=${surfdir}/${sub}/label/lh.MT.thresh.label:label_outline=1:label_color=0,128,0:label_visible=1:label=${surfdir}/${sub}/label/lh.V2.thresh.label:label_outline=1:label_color=128,0,128:label_visible=1:annot=${surfdir}/${sub}/label/lh.aparc.DKTatlas40.annot:annot_outline=1:offset=0,20,0"$cmd_overlay" "$cmd_display$cmd_snapshot
		echo $cmd_surf
		eval $cmd_surf	
done
done
done
done
#-f ${surfdir}/${sub}/surf/lh.pial:name=${sub}-pial:visible=0\
#			:annot=${surfdir}/${sub}/label/lh.aparc.DKTatlas40.annot \
# I could also do cmd_surf cmd_overlay --ss cmdover   NONO  no use

