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
# This is the code for the first step of heudiconv
# you can run this one by itself by uncomment the variables
# or you can also use the qsub code to run them by inputing variables


# basedir=/bcbl/home/public/Gari/VOTCLOC/VSS
# dicom_dirname=dicom
# outputdir=$basedir/BIDS
# sing_path=/bcbl/home/public/Gari/singularity_images

# sample code to run without ses
#singularity run --bind /bcbl/home/public/Gari/VOTCLOC/main_exp:/base --bind /bcbl:/bcbl --bind /export:/export /bcbl/home/public/Gari/singularity_images/heudiconv_1.3.2.sif -d /base/dicom/sub-{subject}/*/*/*/*/* -s 01 02 03 04 05 06 08 -o /bcbl/home/public/Gari/VOTCLOC/main_exp/raw_nifti -f convertall -c none -g all --overwrite > ~/public/Gari/VOTCLOC/main_exp/raw_nifti/log_heudiconv/redo_heudiconv-and-using-subonly/all.log 2> ~/public/Gari/VOTCLOC/main_exp/raw_nifti/log_heudiconv/redo_heudiconv-and-using-subonly/all.err


module load apptainer/latest
echo "Now the singularity is loaded, it is: "
module list


echo "Subject: ${sub} "
echo "Session: ${ses} "
cmd="singularity run \
        	--bind ${basedir}:/base \
	    	--bind /bcbl:/bcbl \
			--bind /export:/export \
        	${sing_path}/heudiconv_1.3.2.sif \
			-d /base/${dicom_dirname}/sub-{subject}/ses-{session}/*/*.dcm \
	    	-s ${sub} \
			-ss ${ses} \
			-o ${outputdir} \
	    	-f convertall \
	    	-c none \
        	-g all \
        	--overwrite "
			# -ss ${ses} \
echo $cmd
eval $cmd

module unload apptainer


# I added this 			-d /base/${dicom_dirname}/sub-{subject}/ses-{session}/*/*.dcm \ is because some of the directory will be read and being processed
