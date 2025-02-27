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
# define some parameters or using qsub to get from upper level 
#codedir=/bcbl/home/public/Gari/VOTCLOC/VSS/code/05_surface_glm

#subs="01 02 03 04 05"
#sess="day1VA day1VB day2VA day2VB"
#sub='S005'
#ses='T01'
fp_name="analysis-MINI_DIPC"
out_name="analysis-MINI_concat"
slice_timing=(0.5)

#for sub in ${subs}; do
#for ses in ${sess}; do
echo "Subject: ${sub} "
echo "Session: ${ses} "
echo "We are going to use smoothed?  ${use_smoothed}"
echo "the smoothing on bold is ${sm} fwhm"
echo "get the codedir": ${codedir}
source ~/tlei/soft/miniconda3/etc/profile.d/conda.sh 
conda activate mini
echo "going to run python"

cmd_smooth="python ${codedir}/src/surface_glm_mini_concat.py -sub ${sub} -ses ${ses} -fp_name ${fp_name} \
-out_name ${out_name} \
-slice_timing ${slice_timing} \
-use_smoothed ${use_smoothed} -sm ${sm} "

cmd_nosmooth="python ${codedir}/src/surface_glm_mini_concat.py -sub ${sub} -ses ${ses} -fp_ana_name ${fp_name} \
-output_name ${out_name} \
-slice_time_ref ${slice_timing} "

echo $cmd_nosmooth
eval $cmd_nosmooth	

#done
#done
