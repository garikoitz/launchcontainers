#!/bin/bash
#!/bin/bash

echo $sub

module load singularity/3.5.2

echo "Starting singularity, using:"
echo "Path: ${path2subderivatives}"
echo "Config: ${path2config}"
date;
singularity run \
        --bind /bcbl:/bcbl \
        --bind ${path2subderivatives}/input:/flywheel/v0/input:ro \
        --bind ${path2subderivatives}/output:/flywheel/v0/output \
        --bind ${path2config}:/flywheel/v0/config.json \
        ~/glerma/software/rtppreproci:1.0.8

echo "ended singularity"

date;
