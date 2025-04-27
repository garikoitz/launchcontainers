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
subs="S002 S003 S005 S006 S007 S008 S009 S010 S009 \
      S010 S011 S012 S014 S015 S016 S017 S019 \
      S020 S021 S022 S023 S024 S025 S026 S027 S028 \
      S030 S031 S033 S034 S035 \
      S041 S042 S043 S044 \
      S045 S046 S047 S049 S050 S051 S052 S053 S054 \
      S055 S057 S058 S059 S060 S061 S062 S063 S064 \
      S065 S066 S068 S069 S070 S071"
surfdir="/bcbl/home/public/Gari/MINI/BIDS_BLQfunc_T1/derivatives/fmriprep/analysis-MINI_DIPC/sourcedata/freesurfer"

export SUBJECTS_DIR=${surfdir}

module load freesurfer/7.3.2
# for mri_label2label the srclabel needs to be full absolute path, and the target label just need to be the simple label name
# mri_label2label --srcsubject S001 --srclabel $SUBJECTS_DIR/S001/label/lh.BA1.label --trgsubject S002 --trglabel what.001-002.label --regmethod surface --hemi lh

###########################################################
###########################################################
###########################################################
for sub in ${subs}; do
cmd="mri_label2label \
	--srclabel ${surfdir}/fsaverage/label/lh.biggerLOTS.label \
	--srcsubject fsaverage \
	--trglabel lh.biggerLOTS.label \
	--trgsubject sub-${sub} \
	--regmethod surface
	--hemi lh "
echo ${cmd}
eval ${cmd}

done
