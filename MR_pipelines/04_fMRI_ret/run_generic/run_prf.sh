#!/bin/bash

#==============================================================================
# Script: run_prf.sh
# Description: Launch PRF analysis pipeline with configurable steps
# Usage: ./run_prf.sh <cluster> <step> <log_note> <subses_name> [task]
#==============================================================================

# Function to display usage
usage() {
    echo "Usage: $0 <cluster> <step> <log_note> <subses_name> [task]"
    echo ""
    echo "Arguments:"
    echo "  cluster      - Cluster to run on (DIPC or BCBL)"
    echo "  step         - Pipeline step to run"
    echo "  log_note     - Note for log directory naming"
    echo "  subses_name  - Name of subject-session list file"
    echo "  task         - (Optional) Task name, defaults to step-specific default"
    echo ""
    echo "Available clusters:"
    echo "  DIPC - Uses SLURM scheduler at /scratch/tlei/VOTCLOC"
    echo "  BCBL - Uses SGE scheduler at /bcbl/home/public/Gari/VOTCLOC/main_exp"
    echo ""
    echo "Available steps:"
    echo "  prfprepare       - Prepare PRF data"
    echo "  prfanalyze-vista - Analyze PRF data with VISTA"
    echo "  prfresult        - Generate PRF results"
    echo ""
    echo "Task options (for prfanalyze-vista):"
    echo "  retCB, retRW, retFF, retfixRW, retfixFF, etc."
    echo ""
    echo "Examples:"
    echo "  $0 DIPC prfprepare exp1 subjects_list.csv"
    echo "  $0 BCBL prfanalyze-vista exp1 subseslist_votcloc.txt retFF"
    echo "  $0 DIPC prfresult exp1 subjects_list.csv"
    exit 1
}

# Check if minimum arguments are provided
if [ $# -lt 4 ]; then
    echo "Error: Missing required arguments"
    echo ""
    usage
fi

cluster="$1"
step="$2"
log_note="$3"
subses_name="$4"
task="${5:-}"  # Optional fifth argument for task

# Validate cluster argument
if [ "$cluster" != "DIPC" ] && [ "$cluster" != "BCBL" ]; then
    echo "Error: Invalid cluster '$cluster'. Must be DIPC or BCBL"
    echo ""
    usage
fi

#==============================================================================
# Set variables based on step
#==============================================================================

case "$step" in
    prfprepare)
        version="1.5.0"
        task="${task:-all}"
        # Cluster-specific settings
        if [ "$cluster" = "DIPC" ]; then
            qos="regular"
            mem="16G"
            cpus="6"
            time="00:20:00"
        else  # BCBL
            queue="short.q"
            mem="8G"
            cpus="6"
            time="00:30:00"
        fi
        ;;
        
    prfanalyze-vista)
        version="2.2.1"
        task="${task:-retFF}"
        # Cluster-specific settings
        if [ "$cluster" = "DIPC" ]; then
            qos="regular"
            mem="32G"
            cpus="20"
            time="10:00:00"
        else  # BCBL
            queue="long.q"
            mem="32G"
            cpus="20"
            time="10:00:00"
        fi
        ;;
        
    prfresult)
        task="${task:-all}"
        # Cluster-specific settings
        if [ "$cluster" = "DIPC" ]; then
            version="0.1.1"
            qos="test"
            mem="16G"
            cpus="10"
            time="00:10:00"
        else  # BCBL
            version="1.0"
            queue="short.q"
            mem="16G"
            cpus="10"
            time="01:00:00"
        fi
        ;;
        
    *)
        echo "Error: Unknown step '$step'"
        echo ""
        usage
        ;;
esac

#==============================================================================
# Define cluster-specific paths
#==============================================================================

if [ "$cluster" = "DIPC" ]; then
    # DIPC paths
    baseP="/scratch/tlei/VOTCLOC"
    script_dir="/scratch/tlei/soft/launchcontainers/MR_pipelines/04_fMRI_ret"
    sif_path="/scratch/tlei/containers/${step}_${version}.sif"
    run_script="$script_dir/run_generic/prfpipeline_generic.sh"
    LOG_DIR="$baseP/dipc_${step}_logs/${log_note}_$(date +"%Y-%m-%d")"
else
    # BCBL paths
    baseP="/bcbl/home/public/Gari/VOTCLOC/main_exp"
    script_dir="/export/home/tlei/tlei/soft/launchcontainers/MR_pipelines/04_fMRI_ret"
    sif_path="/bcbl/home/public/Gari/singularity_images/${step}_${version}.sif"
    run_script="$script_dir/run_generic/prfpipeline_generic.sh"
    LOG_DIR="$baseP/ips_${step}_logs/${log_note}_$(date +"%Y-%m-%d")"
fi

# Common paths
HOME_DIR="$baseP/singularity_home"
license_path="$baseP/BIDS/.license"
json_dir="$baseP/code/${step}_jsons"
code_dir="$baseP/code"
subseslist_path="$code_dir/$subses_name"

#==============================================================================
# Verify paths
#==============================================================================

# Check if subject list exists
if [ ! -f "$subseslist_path" ]; then
    echo "ERROR: Subject list not found: $subseslist_path"
    exit 1
fi

# Check if container exists
if [ ! -f "$sif_path" ]; then
    echo "ERROR: Container not found: $sif_path"
    exit 1
fi

# Check if run script exists
if [ ! -f "$run_script" ]; then
    echo "ERROR: Run script not found: $run_script"
    exit 1
fi

#==============================================================================
# Display Configuration and First Example
#==============================================================================

echo "========================================"
echo "PRF Analysis Configuration"
echo "========================================"
echo "Cluster:      $cluster"
echo "Step:         $step"
echo "Version:      $version"
if [ "$cluster" = "DIPC" ]; then
    echo "QoS:          $qos"
else
    echo "Queue:        $queue"
fi
echo "Memory:       $mem"
echo "CPUs:         $cpus"
echo "Time:         $time"
echo "Task:         $task"
echo "Log note:     $log_note"
echo "Subject list: $subses_name"
echo "========================================"
echo ""
echo "Paths:"
echo "  Base path:    $baseP"
echo "  Container:    $sif_path"
echo "  Subject list: $subseslist_path"
echo "  Run script:   $run_script"
echo "  Log dir:      $LOG_DIR"
echo "  JSON dir:     $json_dir"
echo "  License:      $license_path"
echo "========================================"
echo ""

# Read first valid subject from the list to show example command
line_number=0
example_sub=""
example_ses=""
found_example=false

while IFS=$',' read -r sub ses RUN _; do
    ((line_number++))
    
    # Skip header
    if [ $line_number -eq 1 ]; then
        continue
    fi
    
    # Trim whitespace
    sub=$(echo "$sub" | xargs)
    ses=$(echo "$ses" | xargs)
    RUN=$(echo "$RUN" | xargs)
    
    if [ "$RUN" = "True" ]; then
        example_sub="$sub"
        example_ses="$ses"
        found_example=true
        break
    fi
done < "$subseslist_path"

if [ "$found_example" = true ]; then
    current_time=$(date +"%Y-%m-%d_%H-%M-%S")
    
    echo "Example command for first subject (sub-${example_sub} ses-${example_ses}):"
    echo "----------------------------------------"
    
    if [ "$cluster" = "DIPC" ]; then
        # SLURM example
        log_out="${LOG_DIR}/%J_%x_sub-${example_sub}_ses-${example_ses}_task-${task}_${current_time}.o"
        log_err="${LOG_DIR}/%J_%x_sub-${example_sub}_ses-${example_ses}_task-${task}_${current_time}.e"
        
        echo "sbatch -J ${line_number}_${task}_${step} \\"
        echo "    --time=${time} \\"
        echo "    -n 1 \\"
        echo "    --cpus-per-task=${cpus} \\"
        echo "    --mem=${mem} \\"
        echo "    --partition=general \\"
        echo "    --qos=${qos} \\"
        echo "    -o \"$log_out\" \\"
        echo "    -e \"$log_err\" \\"
        echo "    --export=ALL,CLUSTER=${cluster},STEP=${step},baseP=${baseP},license_path=${license_path},version=${version},sub=${example_sub},ses=${example_ses},json_path=$json_dir/${task}_sub-${example_sub}_ses-${example_ses}.json,sif_path=$sif_path \\"
        echo "    $run_script"
    else
        # SGE example
        qsub_log_out="${LOG_DIR}/sub-${example_sub}_ses-${example_ses}_task-${task}_${current_time}.o"
        qsub_log_err="${LOG_DIR}/sub-${example_sub}_ses-${example_ses}_task-${task}_${current_time}.e"
        
        echo "qsub -N ${task}_${line_number}_${step} \\"
        echo "    -S /bin/bash \\"
        echo "    -q ${queue} \\"
        echo "    -l mem_free=${mem} \\"
        echo "    -o $qsub_log_out \\"
        echo "    -e $qsub_log_err \\"
        echo "    -v CLUSTER=${cluster},STEP=${step},baseP=${baseP},license_path=${license_path},version=${version},sub=${example_sub},ses=${example_ses},json_path=$json_dir/${task}_sub-${example_sub}_ses-${example_ses}.json,sif_path=$sif_path \\"
        echo "    $run_script"
    fi
    echo "----------------------------------------"
else
    echo "WARNING: No valid subjects found in the list (with RUN=True)"
fi

echo ""
echo "========================================"

# Count total jobs that will be submitted
total_jobs=0
line_number=0
while IFS=$',' read -r sub ses RUN _; do
    ((line_number++))
    if [ $line_number -eq 1 ]; then
        continue
    fi
    RUN=$(echo "$RUN" | xargs)
    if [ "$RUN" = "True" ]; then
        ((total_jobs++))
    fi
done < "$subseslist_path"

echo "Total jobs to be submitted: $total_jobs"
echo "========================================"
echo ""

#==============================================================================
# Confirmation prompt
#==============================================================================

read -p "Do you want to proceed with job submission? (y/N): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo ""
    echo "Job submission cancelled by user."
    exit 0
fi

echo ""
echo "Proceeding with job submission..."
echo ""

#==============================================================================
# Create directories
#==============================================================================

mkdir -p "$LOG_DIR"
mkdir -p "$HOME_DIR"

# Copy subseslist to log dir for record
cp "$subseslist_path" "$LOG_DIR/"

echo "Directories created:"
echo "  Log dir:  $LOG_DIR"
echo "  Home dir: $HOME_DIR"
echo "  Subject list copied to log dir"
echo ""

#==============================================================================
# Process subject list and submit jobs
#==============================================================================

echo "Processing subject list..."
echo ""

# Initialize counters
line_number=0
submitted_jobs=0
skipped_jobs=0
failed_jobs=0

# Read the file line by line
while IFS=$',' read -r sub ses RUN _; do
    # Increment line counter
    ((line_number++))

    # Skip the first line which is the header
    if [ $line_number -eq 1 ]; then
        continue
    fi

    # Trim whitespace from variables
    sub=$(echo "$sub" | xargs)
    ses=$(echo "$ses" | xargs)
    RUN=$(echo "$RUN" | xargs)

    current_time=$(date +"%Y-%m-%d_%H-%M-%S")
    
    if [ "$RUN" = "True" ]; then
        # Construct scheduler-specific command
        if [ "$cluster" = "DIPC" ]; then
            # SLURM submission
            log_out="${LOG_DIR}/%J_%x_sub-${sub}_ses-${ses}_task-${task}_${current_time}.o"
            log_err="${LOG_DIR}/%J_%x_sub-${sub}_ses-${ses}_task-${task}_${current_time}.e"
            
            cmd="sbatch -J ${line_number}_${task}_${step} \
                --time=${time} \
                -n 1 \
                --cpus-per-task=${cpus} \
                --mem=${mem} \
                --partition=general \
                --qos=${qos} \
                -o \"$log_out\" \
                -e \"$log_err\" \
                --export=ALL,CLUSTER=${cluster},STEP=${step},baseP=${baseP},license_path=${license_path},version=${version},sub=${sub},ses=${ses},json_path=$json_dir/${task}_sub-${sub}_ses-${ses}.json,sif_path=$sif_path \
                $run_script"
        else
            # SGE submission
            qsub_log_out="${LOG_DIR}/sub-${sub}_ses-${ses}_task-${task}_${current_time}.o"
            qsub_log_err="${LOG_DIR}/sub-${sub}_ses-${ses}_task-${task}_${current_time}.e"
            
            cmd="qsub -N ${task}_${line_number}_${step} \
                -S /bin/bash \
                -q ${queue} \
                -l mem_free=${mem} \
                -o $qsub_log_out \
                -e $qsub_log_err \
                -v CLUSTER=${cluster},STEP=${step},baseP=${baseP},license_path=${license_path},version=${version},sub=${sub},ses=${ses},json_path=$json_dir/${task}_sub-${sub}_ses-${ses}.json,sif_path=$sif_path \
                $run_script"
        fi

        # Print and execute the command
        echo "[$line_number] Submitting job for sub-${sub} ses-${ses} task-${task}"
        eval $cmd
        
        if [ $? -eq 0 ]; then
            echo "    ✓ Job submitted successfully"
            ((submitted_jobs++))
        else
            echo "    ✗ Job submission failed"
            ((failed_jobs++))
        fi
        echo ""
    else
        echo "[$line_number] Skipping sub-${sub} ses-${ses} (RUN=${RUN})"
        ((skipped_jobs++))
    fi
done < "$subseslist_path"

#==============================================================================
# Summary
#==============================================================================

echo ""
echo "========================================"
echo "Job Submission Summary"
echo "========================================"
echo "Cluster:               $cluster"
echo "Step:                  $step"
echo "Task:                  $task"
echo "----------------------------------------"
echo "Total lines processed: $((line_number - 1))"
echo "Jobs submitted:        $submitted_jobs"
echo "Jobs skipped:          $skipped_jobs"
echo "Jobs failed:           $failed_jobs"
echo "========================================"
echo ""
if [ "$cluster" = "DIPC" ]; then
    echo "Monitor jobs with:     squeue -u $USER"
    echo "Cancel jobs with:      scancel <job_id>"
else
    echo "Monitor jobs with:     qstat"
    echo "Cancel jobs with:      qdel <job_id>"
fi
echo "Logs located in:       $LOG_DIR"
echo "========================================"

# Exit with appropriate status
if [ $failed_jobs -gt 0 ]; then
    exit 1
else
    exit 0
fi