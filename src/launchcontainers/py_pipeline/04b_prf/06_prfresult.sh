#!/bin/bash
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
code_dir=/export/home/tlei/tlei/soft/launchcontainers/src/launchcontainers/py_pipeline/04b_prf
LOG_DIR=$baseP/BIDS/derivatives/prfresult/prfresult_logs
HOME_DIR=$baseP/singularity_home
version='0.1.1'
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

if [ -e $code_dir/prfanalyze-vista.json ]; then
	echo "$code_dir/prfanalyze-vista.json file is there"
else
	echo "file doesn't exist"
fi

cmd="unset PYTHONPATH; singularity run \
	-B /bcbl:/bcbl
	-B /export:/export
	-H $baseP/singularity_home \
	-B /bcbl:/bcbl \
	-B /export:/export \
        -B $baseP/BIDS/derivatives:/flywheel/v0/data/derivatives \
        -B $baseP/BIDS:/flywheel/v0/BIDS  \
	-B $code_dir/prfreport.json:/flywheel/v0/config.json \
	--cleanenv /bcbl/home/public/Gari/singularity_images/prfresult_${version}.sif \
	--verbose \
	> ${LOG_DIR}/prfresult_${version}_${current_time}.o 2> ${LOG_DIR}/prfresult_${version}_${current_time}.e "
echo $cmd
eval $cmd
