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
codedir=/bcbl/home/public/Gari/VOTCLOC/VSS/code/05_surface_glm

subs="01 02 03 04"
sess="day1VA day1VB day2VA day2VB" #day1VA day1VB day2VA day2VB day3PF day5BCBL day6BCBL
#sub='S005'
#ses='T01'
# okazaki fmriprep: analysis-okazaki_ST05
# tamagawa and BCBL magnitude fmriprep: analysis-BCBL2_magn_fmap_nochangedtype"
# the nordic tama and BCBL: analysis-rerun_nordic_fmap
fp_name="analysis-okazaki_ST05"
out_name="analysis-okazaki"
slice_timing=(0.5)
sm="02"

for sub in ${subs}; do
for ses in ${sess}; do
#for sm in ${smm}; do
echo "Subject: ${sub} "
echo "Session: ${ses} "
echo "We are going to use smoothed?  ${use_smoothed}"
#echo "the smoothing on bold is ${sm} fwhm"
echo "get the codedir": ${codedir}
source ~/tlei/soft/miniconda3/etc/profile.d/conda.sh
conda activate mini
echo "going to run python"

# two python files:
# 1 fsnative 1 fsaverage
cmd_smooth_native="python ${codedir}/src/surface_glm_votcloc_fsnative.py -sub ${sub} -ses ${ses} -fp_ana_name ${fp_name} \
-output_name ${out_name}_fwhm${sm} \
-slice_time_ref ${slice_timing} \
-use_smoothed -sm ${sm} "

cmd_nosmooth_native="python ${codedir}/src/surface_glm_votcloc_fsnative.py -sub ${sub} -ses ${ses} -fp_ana_name ${fp_name} \
-output_name ${out_name}_nosmooth \
-slice_time_ref ${slice_timing} "

# the smooth on fsaverage is not working, need to check in the future
# cmd_smooth_average="python ${codedir}/src/surface_glm_votcloc_fsaverage.py -sub ${sub} -ses ${ses} -fp_ana_name ${fp_name} \
# -output_name ${out_name}_fwhm${sm}_fsaverage_bold \
# -slice_time_ref ${slice_timing} \
# -use_smoothed -sm ${sm} "

cmd_nosmooth_average="python ${codedir}/src/surface_glm_votcloc_fsaverage.py -sub ${sub} -ses ${ses} -fp_ana_name ${fp_name} \
-output_name ${out_name}_nosmooth_fsaverage_bold \
-slice_time_ref ${slice_timing} "

echo $cmd_nosmooth_average
eval $cmd_nosmooth_average
done
done
#done
