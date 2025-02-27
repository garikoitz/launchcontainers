#!/bin/sh
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
baseP=/bcbl/home/public/Gari/VOTCLOC/main_exp
code_dir=/bcbl/home/home_n-z/tlei/soft/MRIworkflow/04b_prf
LOG_DIR=$baseP/BIDS/derivatives/prfanalyze-vista/prfanalyze-vista_logs
HOME_DIR=$baseP/singularity_home

if [ ! -d $LOG_DIR ]; then
	mkdir -p $LOG_DIR
fi
if [ ! -d $HOME_DIR ]; then
	mkdir -p $HOME_DIR
fi
current_time=$(date +"%Y-%m-%d_%H-%M-%S")

module load apptainer

if [ -e $code_dir/prfanalyze-vista.json ]; then
	echo "$code_dir/prfanalyze-vista.json file is there"
else
	echo "file doesn't exist"
fi

cmd="unset PYTHONPATH; singularity run \
	-H $HOME_DIR \
	-B /bcbl:/bcbl \
	-B /export:/export \
	-B $baseP:/flywheel/v0/input \
	-B $baseP:/flywheel/v0/output  \
	-B $code_dir/prfanalyze-vista.json:/flywheel/v0/input/config.json \
	--cleanenv /bcbl/home/public/Gari/singularity_images/prfanalyze-vista_2.2.1.sif \
	--verbose \
	> ${LOG_DIR}/prfanalyze_${current_time}.o 2> ${LOG_DIR}/prfanalyze_${current_time}.e "
echo $cmd
echo "backup the prfanalyze-vista json to log dir"
cp $code_dir/prfanalyze-vista.json $LOG_DIR
eval $cmd

