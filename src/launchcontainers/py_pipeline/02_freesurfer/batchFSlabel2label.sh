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

