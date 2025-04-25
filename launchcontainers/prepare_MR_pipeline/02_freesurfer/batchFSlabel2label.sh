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
subs="01 02 03 04 05"

surfdir="/bcbl/home/public/Gari/VOTCLOC/derivatives/fmriprep/analysis-okazaki_ST05/sourcedata/freesurfer"

export SUBJECTS_DIR=${surfdir}

module load freesurfer/7.3.2
# for mri_label2label the srclabel needs to be full absolute path, and the target label just need to be the simple label name
# mri_label2label --srcsubject S001 --srclabel $SUBJECTS_DIR/S001/label/lh.BA1.label --trgsubject S002 --trglabel what.001-002.label --regmethod surface --hemi lh

###########################################################
###########################################################
###########################################################
for sub in ${subs}; do
cmd="mri_label2label \
	--srclabel ${surfdir}/fsaverage/label/lh.LOTS.label \
	--srcsubject fsaverage \
	--trglabel lh.LOTS.label \
	--trgsubject sub-${sub} \
	--regmethod surface
	--hemi lh "
echo ${cmd}
eval ${cmd}

done
