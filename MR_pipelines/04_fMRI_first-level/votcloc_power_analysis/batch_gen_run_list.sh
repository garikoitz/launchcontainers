#!/bin/bash
# -----------------------------------------------------------------------------
# Generate run combinations for power analysis
# This script generates run_combinations files for num_of_runs from 1 to 10
# -----------------------------------------------------------------------------

# Configuration
code_dir='/bcbl/home/home_n-z/tlei/soft/launchcontainers/MR_pipelines/04_fMRI_first-level/votcloc_power_analysis'
output_dir='/bcbl/home/public/Gari/VOTCLOC/main_exp/code'
python_script=$code_dir/gen_run_list.py
total_runs=10
seed=42

# Create output directory if it doesn't exist
mkdir -p $output_dir

echo "=================================================="
echo "Generating run combinations"
echo "Output directory: $output_dir"
echo "Total runs available: $total_runs"
echo "Random seed: $seed"
echo "=================================================="
echo ""

# Loop from 1 to 10 runs
for num_of_runs in {1..10}; do
    output_file="${output_dir}/run_list_${num_of_runs}_run.txt"
    
    echo "Generating combinations for ${num_of_runs} run(s)..."
    
    cmd="python $python_script \
        -num_of_runs $num_of_runs \
        -total_runs $total_runs \
        -output $output_file \
        -seed $((seed + num_of_runs)) "
    
    echo $cmd
    eval $cmd
    echo ""
done

echo "=================================================="
echo "All run combinations generated!"
echo "=================================================="
echo ""
echo "Generated files:"
ls -lh ${output_dir}/run_list_*_run.txt