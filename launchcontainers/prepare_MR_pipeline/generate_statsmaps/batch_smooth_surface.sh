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
surfdir="/bcbl/home/public/Gari/MINI/BIDS_BLQfunc_T1/derivatives/fmriprep/analysis-MINI_DIPC/sourcedata/freesurfer"

export SUBJECTS_DIR=${surfdir}
eval "cd /bcbl/home/public/Gari/MINI/BIDS_BLQfunc_T1/derivatives/fmriprep/analysis-MINI_DIPC/sourcedata/freesurfer"

smms="1 2 3 4 5"
echo "the smoothing is going to start"
echo "current directory is $PWD"
contrast_names="RWvsLEX RWvsPER RWvsLEXnoPW RWvsAllnoWords RWvsAllnoWordsnoFaces RWvsAllnoWordsnoFacenoPW \
WordHighvsLEX WordHighvsPER WordHighvsLEXnoPW WordHighvsAllnoWords WordHighvsAllnoWordsnoFaces WordHighvsAllnoWordsnoFacenoPW \
WordLowvsLEX WordLowvsPER WordLowvsLEXnoPW WordLowvsAllnoWords WordLowvsAllnoWordsnoFaces WordLowvsAllnoWordsnoFacenoPW FacevsLEX FacevsPER FacevsLEXnoPW "

stats="z"

if [ ! -d ${outputdir} ]; then mkdir -p ${outputdir}; fi

module load freesurfer/7.3.2

for sm in ${smms}; do
	outputdir="/bcbl/home/public/Gari/MINI/BIDS_BLQfunc_T1/derivatives/l1_surface_smooth_${sm}/${sub}"
	if [ ! -d ${outputdir} ]; then mkdir -p ${outputdir}; fi
	# generate the overlay command and concat it with surf cmd
	overlaydir="/bcbl/home/public/Gari/MINI/BIDS_BLQfunc_T1/derivatives/l1_surface/${sub}"
	labeldir="${surfdir}/sub-${sub}/label"
	#sub-S003_ses-T01_task-MINIblock_hemi-L_space-fsnative_contrast-RWvsLEX_stat-z_statmap.func.gii
	for contrast in $contrast_names; do

	srcval="${overlaydir}/sub-${sub}_ses-${ses}_task-MINIblock_hemi-L_space-fsnative_contrast-${contrast}_stat-${stats}_statmap.func.gii"
	targval="${outputdir}/sub-${sub}_ses-${ses}_task-MINIblock_hemi-L_space-fsnative_contrast-${contrast}_stat-${stats}_statmap_smooth-${sm}.func.gii"
	cmd="mri_surf2surf --hemi lh --s sub-${sub} --fwhm ${sm} --cortex --sval $srcval --tval $targval "
	echo $cmd
	eval $cmd
	# cp the z things out of the overlaydir
	eval "mkdir -p /bcbl/home/public/Gari/MINI/BIDS_BLQfunc_T1/derivatives/l1_surface_onlyz/${sub}"
	target="/bcbl/home/public/Gari/MINI/BIDS_BLQfunc_T1/derivatives/l1_surface_onlyz/${sub}/sub-${sub}_ses-${ses}_task-MINIblock_hemi-L_space-fsnative_contrast-${contrast}_stat-${stats}_statmap.func.gii"
	cmd2="cp $srcval $target"
	eval $cmd2
done
done
