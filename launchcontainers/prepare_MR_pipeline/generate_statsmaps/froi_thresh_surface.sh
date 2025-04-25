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
## Define important paths and parameters
bids="/bcbl/home/public/Gari/MINI/BIDS_BLQfunc_T1"
freesurfer_dir="/bcbl/home/public/Gari/MINI/BIDS_BLQfunc_T1/derivatives/fmriprep/analysis-MINI_DIPC/sourcedata/freesurfer"

export SUBJECTS_DIR=${freesurfer_dir} # Tell FreeSurfer where subjects live
module load workbench/1.4.2
module load freesurfer/7.3.2

subject="S005" # Subject name
ses="T01"
hemi="L" # Hemisphere name in fMRIPrep naming convention
space="fsnative" # Space of statmaps
contrast="RWvsLEX" # GLM contrast name
stat="z" # Which stat to threshold
sm=2 # 1 2 3 4 5
smoothed_label="smooth-$sm" # (leave blank if not smoothing)

outdir=${bids}/derivatives/threshold_fROIs/${subject} # Where outputs will go
mkdir -p ${outdir} # Make the output directory

region_label="$freesurfer_dir/sub-${subject}/label/lh.SP_FG24LOTS.label" # A FS label file that defines the searchspace
label_name="FG24LOTS" # A descriptive name for the label
value_percentile="90" # Percentile to threshold statistic

l1_gifti=${bids}/derivatives/l1_surface_smooth_$sm # Where statmaps from surface GLM code are

# Locate the statistical map
statmap=${l1_gifti}/${subject}/sub-${subject}_ses-${ses}_task-MINIblock_hemi-${hemi}_space-${space}_contrast-${contrast}_stat-${stat}_statmap_${smoothed_label}.func.gii # Path to statmap

## Get hemisphere name in FreeSurfer naming convention
if [ "$hemi" == "L" ];
    then hemi_fs="lh"; # Hemi name in FreeSurfer conventions; "lh" or "rh"
elif [ "$hemi" == "R" ];
    then hemi_fs="rh"; # Hemi name in FreeSurfer conventions; "lh" or "rh"
fi;

## Convert FS label to GIFTI
region_gii=${outdir}/sub-${subject}_ses-${ses}_task-MINIblock_hemi-${hemi}_space-${space}_desc-${label_name}_roi.func.gii
mris_convert --label ${region_label} ${label_name} \
    ${freesurfer_dir}/sub-${subject}/surf/${hemi_fs}.white ${region_gii}

## Mask statmap by label
masked_statmap=${outdir}/${subject}_hemi-${hemi}_space-${space}_contrast-${contrast}_stat-${stat}_desc-${label_name}_desc-masked_roi.func.gii
#masked_statmap="statmap_masked.func.gii" # Output variable for next command
~/tlei/soft/workbench/bin_rh_linux64/wb_command -metric-mask ${statmap} ${region_gii} ${masked_statmap}

## Find threshold corresponding to top X% of values in ROI, save value as "thresh"
~/tlei/soft/workbench/bin_rh_linux64/wb_command -metric-stats ${statmap} -roi ${region_gii} -percentile ${value_percentile} | read thresh
echo "${value_percentile} percentile of ${stat} statistic within ${hemi} ${label_name} is ${thresh}"

## Make a binary mask of values above that threshold
# Output variable for next command
thresholded_statmap=${outdir}/${subject}_hemi-${hemi}_space-${space}_contrast-${contrast}_stat-${stat}_desc-${label_name}_desc-masked_desc-thresholded_roi.func.gii
# Binarize and threshold the statmap
~/tlei/soft/workbench/bin_rh_linux64/wb_command -metric-math "(statmap > ${thresh})" ${thresholded_statmap} -var statmap ${masked_statmap}
