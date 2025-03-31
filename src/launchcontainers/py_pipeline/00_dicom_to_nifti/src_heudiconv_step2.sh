## -----------------------------------------------------------------------------
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
# basedir=/bcbl/home/public/Gari/VOTCLOC/VSS
# dicom_dirname=dicom
# outputdir=$basedir/BIDS
# sing_path=/bcbl/home/public/Gari/singularity_images
# sub='05'
# ses='day6BCBL'
# heuristicfile=$basedir/code/00_dicom_to_nifti/heuristic/heuristic_BCBL.py

module load apptainer/latest
echo "Now the singularity is loaded, it is: "
module list


echo "Subject: ${sub} "
# try the no session thing Feb 09 2025
echo "Session: ${ses} "
cmd="singularity run \
        	--bind ${basedir}:/base \
	    	--bind /bcbl:/bcbl \
			--bind /export:/export \
        	${sing_path} \
			-d /base/${dicom_dirname}/sub-{subject}/ses-{session}/*/*.dcm \
	    	--subjects ${sub} \
			--ses ${ses} \
			-o ${outputdir} \
        	--overwrite \
	    	-f ${heuristicfile} \
	    	-c dcm2niix \
	    	-b \
        	--grouping all "
			# try the no sesion
			#--ses ${ses} \
echo $cmd
eval $cmd

module unload apptainer
