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
#subs="fsaverage"
# BIDS ID
subs="01 02 03 04 05"

surfdir="/bcbl/home/public/Gari/VOTCLOC/derivatives/fmriprep/analysis-okazaki_ST05/sourcedata/freesurfer"

export SUBJECTS_DIR=${surfdir}

###########################################################
###########################################################
###########################################################
module load freesurfer/7.3.2

for sub in ${subs}; do
cmd="mri_annotation2label --subject sub-${sub} \
	--hemi lh \
	--label 7 \
	--annotation aparc.DKTatlas \
	--outdir ${surfdir}/sub-${sub}/label \
	--surface inflated "
eval ${cmd}
cmd="mri_annotation2label --subject sub-${sub} \
	--hemi lh \
	--label 9 \
	--annotation aparc.DKTatlas \
	--outdir ${surfdir}/sub-${sub}/label \
	--surface inflated "
eval ${cmd}
cmd="mri_annotation2label --subject sub-${sub} \
	--hemi lh \
	--label 11 \
	--annotation aparc.DKTatlas \
	--outdir ${surfdir}/sub-${sub}/label \
	--surface inflated "
eval ${cmd}
done


# MINI ID
#subs="S001 S002 S003 S005 S006 S007 S008 S009 S010 S009 S010 S011 S012 S014 S015 S016 S017 S019\
#      S020 S021 S022 S023 S024 S025 S026 S027 S028 \
#      S030 S031 S033 S034 S035 S041 S042 S043 S044 \
#      S045 S046 S047 S049 S050 S051 S052 S053 S054 \
#      S055 S057 S058 S059 S060 S061 S062 S063 S064 \
#      S065 S066 S068 S069 S070 S101"


# BIDS ID
#subs="S002 S003 S005 S006 S007 S008 S009 S010 S009 \
#      S010 S011 S012 S014 S015 S016 S017 S019 \
#      S020 S021 S022 S023 S024 S025 S026 S027 S028 \
#      S030 S031 S033 S034 S035 \
#      S041 S042 S043 S044 \
#      S045 S046 S047 S049 S050 S051 S052 S053 S054 \
#      S055 S057 S058 S059 S060 S061 S062 S063 S064 \
#      S065 S066 S068 S069 S070 S071"

