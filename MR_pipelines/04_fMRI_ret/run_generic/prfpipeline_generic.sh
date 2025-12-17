#!/bin/bash

#==============================================================================
# Universal PRF analysis run script
# Handles: prfprepare, prfanalyze-vista, prfresult
# Auto-detects cluster (DIPC or BCBL) and adjusts bindings accordingly
# Determines step from script name or STEP environment variable
#==============================================================================

# Determine the step from the script name or environment variable
if [ -z "$STEP" ]; then
    SCRIPT_NAME=$(basename "$0")
    STEP=$(echo "$SCRIPT_NAME" | sed 's/_generic.sh//')
fi

# Validate step
if [ "$STEP" != "prfprepare" ] && [ "$STEP" != "prfanalyze-vista" ] && [ "$STEP" != "prfresult" ]; then
    echo "ERROR: Unknown step '$STEP'"
    echo "Valid steps: prfprepare, prfanalyze-vista, prfresult"
    exit 1
fi

# Detect cluster based on environment variables or passed CLUSTER variable
if [ -z "$CLUSTER" ]; then
    # Auto-detect based on hostname or paths
    if [ -d "/scratch/tlei/VOTCLOC" ]; then
        CLUSTER="DIPC"
    elif [ -d "/bcbl/home/public/Gari/VOTCLOC" ]; then
        CLUSTER="BCBL"
    else
        echo "ERROR: Cannot detect cluster. Please set CLUSTER variable."
        exit 1
    fi
fi

# Set cluster-specific configurations
if [ "$CLUSTER" = "DIPC" ]; then
    MODULE_NAME="Apptainer/1.2.4"
    BIND_PATHS="-B /scratch:/scratch -B /data:/data"
else
    MODULE_NAME="apptainer"
    BIND_PATHS="-B /bcbl:/bcbl -B /export:/export"
fi

# Load module
module load ${MODULE_NAME}

# Set up temp directories for prfanalyze-vista (needs local temp storage)
if [ "$STEP" = "prfanalyze-vista" ]; then
    # Detect job ID from SLURM or SGE
    if [ ! -z "$SLURM_JOB_ID" ]; then
        JOB_ID=$SLURM_JOB_ID
    elif [ ! -z "$JOB_ID" ]; then
        JOB_ID=$JOB_ID
    else
        JOB_ID=$$
    fi
    
    export APPTAINER_TMPDIR=/tmp/$JOB_ID
    mkdir -p $APPTAINER_TMPDIR
    
    export APPTAINER_CACHEDIR=/tmp/$USER/apptainer_cache
    mkdir -p $APPTAINER_CACHEDIR
fi

# Build step-specific bindings
case "$STEP" in
    prfprepare)
        # prfprepare: fmriprep input, derivatives output, BIDS, and FreeSurfer license
        STEP_BINDS="-B $baseP/BIDS/derivatives/fmriprep:/flywheel/v0/input \
                    -B $baseP/BIDS/derivatives:/flywheel/v0/output \
                    -B $baseP/BIDS:/flywheel/v0/BIDS \
                    -B $json_path:/flywheel/v0/config.json \
                    -B $license_path:/opt/freesurfer/.license"
        VERBOSE_FLAG=""
        ;;
        
    prfanalyze-vista)
        # prfanalyze-vista: standard input/output with verbose
        STEP_BINDS="-B $baseP:/flywheel/v0/input \
                    -B $baseP:/flywheel/v0/output \
                    -B $json_path:/flywheel/v0/input/config.json"
        VERBOSE_FLAG="--verbose"
        ;;
        
    prfresult)
        # prfresult: derivatives data and BIDS with verbose
        STEP_BINDS="-B $baseP/BIDS/derivatives:/flywheel/v0/data/derivatives \
                    -B $baseP/BIDS:/flywheel/v0/BIDS \
                    -B $json_path:/flywheel/v0/config.json"
        VERBOSE_FLAG="--verbose"
        ;;
esac

# Construct singularity command
cmd="unset PYTHONPATH; singularity run \
	${BIND_PATHS} \
	-H $baseP/singularity_home \
	${STEP_BINDS} \
	--cleanenv ${sif_path} \
	${VERBOSE_FLAG}"

# Display execution information
echo "========================================"
echo "Step:     $STEP"
echo "Cluster:  $CLUSTER"
echo "Module:   $MODULE_NAME"
if [ "$STEP" = "prfanalyze-vista" ]; then
    echo "Temp dir: $APPTAINER_TMPDIR"
fi
echo "========================================"
echo "This is the command running :$cmd"
echo "start running ####################"

# Execute the command
eval $cmd

# Capture exit status
EXIT_STATUS=$?

echo ""
echo "========================================"
echo "Execution completed with exit status: $EXIT_STATUS"
echo "========================================"

# Cleanup temp directory for prfanalyze-vista
if [ "$STEP" = "prfanalyze-vista" ] && [ -d "$APPTAINER_TMPDIR" ]; then
    rm -rf $APPTAINER_TMPDIR
fi

# Unload module
module unload ${MODULE_NAME}

exit $EXIT_STATUS