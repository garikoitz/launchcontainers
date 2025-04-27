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
basedir=/bcbl/home/public/Gari/MINI/BIDS_BLQfunc_T1
logdir=$basedir/logdir/l1_surfaces_log/glm_merged
codedir=/bcbl/home/public/Gari/VOTCLOC/VSS/code/05_surface_glm

#subseslist_path=$codedir/subseslist.tsv
subseslist_path=/bcbl/home/public/Gari/MINI/BIDS_BLQfunc_T1/code/l1_glm/subseslist.tsv


mkdir -p $logdir
# Initialize a line counter
line_number=0

use_smoothed=False
#sms="01 02 03 04 05 010"

#for sm in $sms ; do
# Read the file line by line
while IFS=$'\t' read -r sub ses
do
    # Increment line counter
    ((line_number++))

    # Skip the first line which is the header
    if [ $line_number -eq 1 ]; then
        continue
    fi


	echo "### Runing SURFACE_glm on SUBJECT: $sub $ses SESSION ###"
	cmd="qsub -q long.q \
		-N SURFACE_glm-${sub}_s-${ses} \
		-o $logdir/SURFACE_glm-${sub}-${ses}.o \
    	-e $logdir/SURFACE_glm-${sub}-${ses}.e \
		-l mem_free=16G \
		-v sub=${sub} \
		-v ses=${ses} \
		-v use_smoothed=${use_smoothed} \
		-v codedir=$codedir \
		$codedir/run_python_glm.sh "

	echo $cmd
	eval $cmd
done < "$subseslist_path"

#done
#-v sm=${sm} \
