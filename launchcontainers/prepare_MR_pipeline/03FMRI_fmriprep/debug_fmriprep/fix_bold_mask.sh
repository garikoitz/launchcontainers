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

# I was thinking at something simpler:
# fmriprep projects the T1w and the functional data in the same spaces (after correcting the functional data and aligning it to the T1w) but usually at different resolutions.

# For example you could use different tools: (FSLeyes - resample, FLIRT (with -applyxfm), niftyreg/reg_resample, AFNI 3dresample, … )
# to resample the image :
# anat/sub-SUB_ses-SES_space-MNI152NLin2009cAsym_desc-brain_mask.nii.gz
# into
# func/sub-SUB_ses-SES_task-TASK_run-RUN_space-MNI152NLin2009cAsym_desc-preproc_bold.nii.gz resolution.



# another thing is using fsl to get the bold brian mask

module load fsl/6.0.3
module load ants/2.3
basedir='/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS/derivatives/fmriprep/analysis-forcenobbr_minimal'
sub='01'
ses='09'
subses_dir=${basedir}/sub-${sub}/ses-${ses}
anat_mask=${subses_dir}/anat/sub-${sub}_ses-${ses}_run-01_desc-brain_mask.nii.gz
boldref=${subses_dir}/func/sub-${sub}_ses-${ses}_task-fLoc_run-01_desc-hmc_boldref.nii.gz

xfm_txt=${subses_dir}/func/sub-${sub}_ses-${ses}_task-fLoc_run-01_from-boldref_to-T1w_mode-image_desc-coreg_xfm.txt
xfm_mat=${subses_dir}/func/sub-${sub}_ses-${ses}_task-fLoc_run-01_from-boldref_to-T1w_mode-image_desc-coreg_xfm.mat

output_bold_mask=${subses_dir}/func/sub-${sub}_ses-${ses}_task-fLoc_run-01_desc-tigeredit_brain_mask.nii.gz

#cp ${xfm_txt} ${xfm_mat}

# command fsl doesn't work because the trasform matrix is gen by ANTs
cmd_fsl="flirt -in ${anat_mask} \
    -ref ${boldref} \
    -applyxfm -init ${xfm_txt} \
    -out ${output_bold_mask} "

cmd_ants="antsApplyTransforms -d 3 -i ${anat_mask} -r ${boldref} \
    -t ${xfm_txt}\
    -o ${output_bold_mask} -n NearestNeighbor"

echo "Conducting bold mask fixing on sub- ${sub} ses- ${ses}"
echo "The command is   ${cmd_ants}"
eval $cmd_ants
