
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
# """
# BIDS ID
subs="S042 S043 S065"
#subs="S041 S042 S043 S044 \
#      S045 S046 S047 S049 S050 S051 S052 S053 S054 \
#      S055 S057 S058 S059 S060 S061 S062 S063 S064 \
#      S065 S066 S068 S069 S070 S071"

surfdir="/home/tlei/tlei/MINI/derivatives/freesurfer"

export SUBJECTS_DIR=${surfdir}

# degree of freedom is 297
# p=.05 ct=1.65
# p=.01 ct=2.35
# p=0.001 ct=3.13
p005=1.65
p001=2.35
p0001=3.13

contrast_names="FacevsPER RWvsSD "

#RWvsLEX RWvsPER RWvsLEXnoP RWWvsAllnoWords RWvsAllnoWordsnoFaces RWvsAllnoWordsnoFacenoPW \
#WordHighvsLEX WordHighvsPER WordHighvsLEXnoPW WordHighvsAllnoWords WordHighvsAllnoWordsnoFaces WordHighvsAllnoWordsnoFacenoPW \
#WordLowvsLEX WordLowvsPER WordLowvsLEXnoPW WordLowvsAllnoWords WordLowvsAllnoWordsnoFaces WordLowvsAllnoWordsnoFacenoPW FacevsLEX FacevsPER FacevsLEXnoPW "

search_space="bigLOTS"
space="fsnative"
thresh=1.3
sm=00

#if [ ! -d ${outputdir} ]; then mkdir -p ${outputdir}; fi

# module load freesurfer/7.3.2

cmd_display="-colorscale
-view inferior \
-viewport 3d "

cmd_surf=""

for sub in ${subs}; do
	#for ses in ${sess}; do
		outputdir="/home/tlei/tlei/MINI/derivatives/vertex_wisecount/analysis-MINI_test-retest/tmap/sub-${sub}/pngs"
		if [ ! -d ${outputdir} ]; then mkdir -p ${outputdir}; fi
		# generate the overlay command and concat it with surf cmd
		overlaydir="/home/tlei/tlei/MINI/derivatives/vertex_wisecount/analysis-MINI_test-retest/tmap"
		labeldir="${surfdir}/sub-${sub}/label"
		#sub-S003_ses-T01_task-MINIblock_hemi-L_space-fsnative_contrast-RWvsLEX_stat-z_statmap.func.gii


		for contrast in $contrast_names; do

			cmd_overlay=":overlay=${overlaydir}/sub-${sub}_space-${space}_contrast-${contrast}_desc-thresh${thresh}sm${sm}_vertexwisecount.gii:overlay_threshold=1,2:visible=1:overlay_mask=${labeldir}/lh.bigLOTS.label:overlay_custom:overlay_color=colorwheel,inverse:overlay_opacity=0.4"


			cmd_snapshot="-cam azimuth 30 Elevation -20 Dolly 1.5 -ss ${outputdir}/${contrast}.png "

			cmd_surf="freeview -f ${surfdir}/sub-${sub}/surf/lh.inflated:curvature_method=binary:name=${sub}-inflated:visible=1${cmd_overlay}:label=${labeldir}/lh.bigLOTS.label:label_visible=1:label_color=0,255,255:label_outline=1:offset=-45,10,10 "$cmd_display$cmd_snapshot


			echo $cmd_surf
			eval $cmd_surf

		done
	#done
done

#-f ${surfdir}/${sub}/surf/lh.pial:name=${sub}-pial:visible=0\
#			:annot=${surfdir}/${sub}/label/lh.aparc.DKTatlas40.annot \

#label=${labeldir}/lh.FG2.mpm.vpnl.label:label_outline=1:label_color=0,255,0:label_visible=1:\
#label${labeldir}/lh.FG4.mpm.vpnl.label:label_outline=1:label_color=0,255,255:label_visible=1:\
