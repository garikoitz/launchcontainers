#!/bin/bash

echo $sub
set -x
singularityVersion=3.5.2
module load singularity/$singularityVersion

echo "Starting singularity, using:"
echo "Tool: ${tool}"
echo "Path: ${path2subderivatives}"
echo "Config: ${path2config}"
date;
# Maybe required for Singularity in DIPC, check if necessary
# export SINGULARITY_BIND=""
singularity run -e --no-home \
	--bind /bcbl:/bcbl \
	--bind /tmp:/tmp \
	--bind /scratch:/scratch \
	--bind ${path2subderivatives}/input:/flywheel/v0/input:ro \
	--bind ${path2subderivatives}/output:/flywheel/v0/output \
	--bind ${path2config}:/flywheel/v0/config.json \
	~/glerma/containers/${tool}.sif

echo "ended singularity"

date;

