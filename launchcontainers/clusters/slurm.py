# clusters/slurm.py
from __future__ import annotations
import os
import os.path as op
import subprocess as sp
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd

from launchcontainers import utils as do
from launchcontainers.gen_launch_cmd import gen_RTP2_cmd

def gen_slurm_array_job_script(
            parse_namespace,
            df_subses,
            num_of_true_run,
            run_lc,
):
    """
    Alternative implementation using SLURM array jobs (more efficient).
    
    Args:
        Same as gen_slurm_job_script
        
    Returns:
        Single job ID for the array job (None if dry_run=True)
    """
    # read LC config yml from analysis dir
    analysis_dir = parse_namespace.workdir
    lc_config_fpath = op.join(analysis_dir, 'lc_config.yaml')
    lc_config = do.read_yaml(lc_config_fpath)
    container = lc_config['general']['container']
    analysis_name = lc_config['general']['analysis_name']
    host = lc_config['general']['host']
    jobqueue_config = lc_config['host_options'][host]

    # below is the job specific configs
    job_name = jobqueue_config['job_name']
    cores=jobqueue_config['cores']
    memory=jobqueue_config['memory']
    partition=jobqueue_config['partition']
    qos=jobqueue_config['qos']
    walltime=jobqueue_config['walltime']
    # create log dir and tmp dir to store tmp .slurm file and the container logs
    log_dir = f"{analysis_dir}/dipc_{container}_logs/{analysis_name}_{datetime.now().strftime('%Y-%m-%d')}"  
    
    # Read subject/session list
    print(f"Loaded {num_of_true_run} subjects/sessions for array job")
    # Create commands file
    commands_file = f"{log_dir}/commands_{job_name}.txt"
    with open(commands_file, 'w') as f:
        for idx, row in df_subses.iterrows():
            sub = str(row['sub']).zfill(2)
            ses = str(row['ses']).zfill(2)
            cmd = gen_RTP2_cmd(lc_config, sub, ses, analysis_dir)
            f.write(f"{cmd}\n")
    
    # Generate array job script
    job_name = f"{job_name}_array"
    script_content = f"""#!/bin/bash
    #SBATCH --job-name={job_name}
    #SBATCH --output={log_dir}/{job_name}_%A_%a.out
    #SBATCH --error={log_dir}/{job_name}_%A_%a.err
    #SBATCH --array=1-{len(df_subses)}
    #SBATCH --time={walltime}
    #SBATCH --cpus-per-task={cores}
    #SBATCH --mem={memory}
    #SBATCH --partition={partition}
    #SBATCH --qos={qos}

    # Job execution
    set -e

    echo "Starting array task $SLURM_ARRAY_TASK_ID on $(hostname)"
    echo "Job ID: $SLURM_JOB_ID"

    # Get command for this array task
    COMMAND=$(sed -n "${{SLURM_ARRAY_TASK_ID}}p" {commands_file})

    echo "Executing: $COMMAND"
    eval $COMMAND

    echo "Task $SLURM_ARRAY_TASK_ID completed successfully"
    """
    # if not run_lc, will just print the thing out
    if not run_lc:
        print(f"\n=== ARRAY JOB DRY RUN ===")
        print(f"Commands file: {commands_file}")
        print(f"Array size: {len(df_subses)}")
        print(f"Job name: {job_name}")
        print("\nScript content:")
        print(script_content)
        print("=" * 50)
        return None
    
    # Write and submit script
    script_file = f"{log_dir}/{job_name}_script.slurm"
    with open(script_file, 'w') as f:
        f.write(script_content)
    
    try:
        result = sp.run(
            ['sbatch', script_file],
            capture_output=True,
            text=True,
            check=True
        )
        
        job_id = result.stdout.strip().split()[-1]
        print(f"✓ Submitted array job '{job_name}' with ID: {job_id}")
        print(f"✓ Commands file: {commands_file}")
        print(f"✓ Script file: {script_file}")
        print(f"✓ Array tasks: 1-{len(df_subses)}")
        
        return job_id
        
    except sp.CalledProcessError as e:
        print(f"✗ Failed to submit array job: {e.stderr}")
        return None
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return None

# Example usage and testing
if __name__ == "__main__":
    
    # Example parameters (adjust to match your setup)
    baseP = "/scratch/tlei/VOTCLOC"
    subses_list = "/scratch/tlei/VOTCLOC/code/subseslist_jun16.txt"
    
    print("=== Testing gen_sub_ses_cmd ===")
    test_cmd = gen_sub_ses_cmd(baseP, "01", "01")
    print("Generated command:")
    print(test_cmd)
    
    print("\n=== Testing individual jobs (dry run) ===")
    job_ids = gen_slurm_job_script(
        baseP=baseP,
        subses_list_path=subses_list,
        log_note="test_individual",
        dry_run=True
    )
    
    print("\n=== Testing array job (dry run) ===")
    array_job_id = gen_slurm_array_job_script(
        baseP=baseP,
        subses_list_path=subses_list,
        log_note="test_array", 
        dry_run=True
    )