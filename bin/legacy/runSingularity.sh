#!/bin/bash
#PBS -l cput=1000:00:00

# be alart
# variables that are passing to this script from outside layer
# ...
# $sin_ver                  singualrity version to load
# $container                container you want to run
# $tmpdir                   TMP dir for ENV variable pass to singularity
# $tool                     which tool we are running (only for printing aim)
# $path2subderivatives      input and output dir location
# $path2config              config file location
    
module load $sin_ver
# we need following lines for running fixAllSegmentations.m
# (from thalamus segmentation) successfully in DIPC
export SINGULARITYENV_TMPDIR=$tmpdir
export SINGULARITY_BIND=""
TMPDIR=
echo $SINGULARITYENV_TMPDIR
echo $sub
echo "Starting singularity, using:"
echo "Tool: ${tool}"
echo "Path: ${path2subderivatives}"
echo "Config: ${path2config}"
echo "Container: ${container}"
date;
echo "Running: ${sin_ver}"

echo "PSD_ID: ${PBS_JOBID}"
echo "SLURM_JOB_ID: ${SLURM_JOB_ID}"
if [ "$host" == "BCBL" ];then 
 cmd="singularity run -e --no-home \
 	--bind /bcbl:/bcbl \
	--bind /tmp:/tmp \
	--bind /scratch:/scratch \
	--bind ${path2subderivatives}/input:/flywheel/v0/input:ro \
	--bind ${path2subderivatives}/output:/flywheel/v0/output \
	--bind ${path2config}:/flywheel/v0/config.json \
	$container"
    echo $cmd
    eval $cmd
    echo "ended singularity"

elif [ "$host" == "DIPC" ];then

    if [ "$system" == "scratch" ]; then
       cmd="singularity run -e --no-home \
            --bind /scratch:/scratch \
            --bind ${path2subderivatives}/input:/flywheel/v0/input:ro \
            --bind ${path2subderivatives}/output:/flywheel/v0/output \
            --bind ${path2config}:/flywheel/v0/config.json \
            $container" 
        echo $cmd
        eval $cmd
        echo "ended singularity"
 
    
    elif [ "$system" == "lscratch" ]; then
 
        if [ "$manager" == "qsub" ] ; then
            # using TORQUE to submit
            # use LSCRATCH_DIR as temporary dir to do the computation
            # once finished, move the content back to /scratch
            export LSCRATCH_DIR=/lscratch/$USER/jobs/$PBS_JOBID
            mkdir -p $LSCRATCH_DIR/input $LSCRATCH_DIR/output
            export SINGULARITYENV_TMPDIR=/flywheel/v0/output
        elif [ "$manager" == "slurm" ]; then
            # use SLURM to submit
            export LSCRATCH_DIR=/lscratch/$USER/jobs/${SLURM_JOB_ID}
            mkdir -p $LSCRATCH_DIR/input $LSCRATCH_DIR/output
            export SINGULARITYENV_TMPDIR=/flywheel/v0/output
            export SINGULARITYENV_MRTRIX_TMPFILE_DIR=/flywheel/v0/output
        fi
        cmd="singularity run -e --no-home \
     	    --bind /scratch:/scratch \
	        --bind ${LSCRATCH_DIR}/input:/flywheel/v0/input:ro \
    	    --bind ${LSCRATCH_DIR}/output:/flywheel/v0/output \
    	    --bind ${path2config}:/flywheel/v0/config.json \
    	    $container"
        cp -r ${path2subderivatives}/input/* $LSCRATCH_DIR/input/
        echo $cmd
        eval $cmd
        echo "ended singularity"
        export RESULTS_DIR=${path2subderivatives}/output
        echo "### copying from $LSCRATCH_DIR to $RESULTS_DIR/ ###"
        cp -r $LSCRATCH_DIR/output/* $RESULTS_DIR/
        rm -rf  $LSCRATCH_DIR
    fi

fi
date;

