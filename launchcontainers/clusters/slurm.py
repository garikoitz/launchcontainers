# clusters/slurm.py
from __future__ import annotations
import subprocess
import tempfile
import os
import yaml
from typing import List, Optional, Dict, Any

def load_slurm_config(config_path: str) -> Dict[str, Any]:
    """
    Load SLURM configuration from YAML file.
    
    Args:
        config_path: Path to the YAML configuration file
        
    Returns:
        Dictionary containing configuration parameters
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config

def generate_slurm_script_from_config(config: Dict[str, Any]) -> str:
    """
    Generate SLURM job script template from configuration.
    
    Args:
        config: Configuration dictionary loaded from YAML
        job_name: Job name template (can contain placeholders)
        
    Returns:
        SLURM job script template as string
    """
    script_lines = ["#!/bin/bash"]
    job_name=config["job_name"]
    # Basic SLURM directives
    script_lines.append(f"#SBATCH --job-name={job_name}")
    script_lines.append(f"#SBATCH --output={job_name}_%j.out")
    script_lines.append(f"#SBATCH --error={job_name}_%j.err")
    
    # Resource allocation
    if 'cores' in config:
        script_lines.append(f"#SBATCH --cpus-per-task={config['cores']}")
    
    if 'memory' in config:
        script_lines.append(f"#SBATCH --mem={config['memory']}")
    
    if 'walltime' in config:
        script_lines.append(f"#SBATCH --time={config['walltime']}")
    
    # Queue/partition settings
    if 'queue' in config:
        script_lines.append(f"#SBATCH --partition={config['queue']}")
    
    if 'qos' in config:
        script_lines.append(f"#SBATCH --qos={config['qos']}")
    
    # Additional job directives
    if 'job_extra_directives' in config:
        for directive in config['job_extra_directives']:
            if not directive.startswith('--'):
                directive = '--' + directive
            script_lines.append(f"#SBATCH {directive}")
    
    # Add empty line before job execution
    script_lines.append("")
    
    # Set up temporary directory if specified
    if 'tmpdir' in config:
        script_lines.append(f"# Set up temporary directory")
        script_lines.append(f"export TMPDIR={config['tmpdir']}")
        script_lines.append(f"mkdir -p $TMPDIR")
        script_lines.append("")
    
    # Container setup based on configuration
    container_type = config.get('apptainer', 'singularity').lower()
    if container_type in ['apptainer', 'singularity']:
        # Add mount options if specified
        mount_opts = ""
        if 'mount_options' in config and config['mount_options']:
            mount_binds = ','.join(config['mount_options'])
            mount_opts = f" --bind {mount_binds}"
        
        script_lines.append(f"# Container execution")
        if not config.get('use_module', False):
            # Direct container execution
            script_lines.append(f"# Using {container_type} directly")
        else:
            # Module loading approach
            script_lines.append(f"module load {container_type}")
        
        script_lines.append("")
    
    # Add command placeholder
    script_lines.append("# Execute the command")
    script_lines.append("{COMMAND}")
    
    return '\n'.join(script_lines)

def submit_jobs_from_config(commands: List[str], 
                           config_path: str, 
                           job_name_prefix: str = "batch_job",
                           dry_run: bool = False) -> List[str]:
    """
    Submit SLURM batch jobs using configuration from YAML file.
    
    Args:
        commands: List of commands to execute
        config_path: Path to YAML configuration file
        job_name_prefix: Prefix for job names
        dry_run: If True, only show what would be submitted
        
    Returns:
        List of job IDs (empty strings if dry_run=True or submission fails)
    """
    # Load configuration
    config = load_slurm_config(config_path)
    
    # Generate job script template
    script_template = generate_slurm_script_from_config(config, "{JOB_NAME}")
    
    print(f"Loaded configuration from {config_path}")
    print(f"Submitting {len(commands)} jobs with prefix '{job_name_prefix}'")
    
    if dry_run:
        print("\n=== CONFIGURATION ===")
        for key, value in config.items():
            print(f"{key}: {value}")
        print("\n=== GENERATED SCRIPT TEMPLATE ===")
        print(script_template.replace("{JOB_NAME}", f"{job_name_prefix}_001").replace("{COMMAND}", "EXAMPLE_COMMAND"))
        print("=" * 50)
    
    # Submit jobs using the existing function
    return submit_slurm_batch_jobs(commands, script_template, job_name_prefix, dry_run)

def submit_array_job_from_config(commands: List[str],
                                config_path: str,
                                job_name: str = "array_job", 
                                dry_run: bool = False) -> Optional[str]:
    """
    Submit SLURM array job using configuration from YAML file.
    
    Args:
        commands: List of commands to execute
        config_path: Path to YAML configuration file
        job_name: Name for the array job
        dry_run: If True, only show what would be submitted
        
    Returns:
        Job ID (empty string if dry_run=True or submission fails)
    """
    # Load configuration
    config = load_slurm_config(config_path)
    
    # Generate base script template
    base_template = generate_slurm_script_from_config(config, "{JOB_NAME}")
    
    # Modify for array job
    lines = base_template.split('\n')
    
    # Find the line after error directive to add array directive
    for i, line in enumerate(lines):
        if line.startswith('#SBATCH --error='):
            lines.insert(i + 1, '#SBATCH --array=1-{ARRAY_SIZE}')
            break
    
    # Replace the command section for array job
    for i, line in enumerate(lines):
        if line == "{COMMAND}":
            lines[i] = "# Read and execute command based on array task ID"
            lines.insert(i + 1, "COMMAND=$(sed -n \"${SLURM_ARRAY_TASK_ID}p\" {COMMANDS_FILE})")
            lines.insert(i + 2, "eval $COMMAND")
            break
    
    array_template = '\n'.join(lines)
    
    print(f"Loaded configuration from {config_path}")
    print(f"Submitting array job '{job_name}' with {len(commands)} tasks")
    
    if dry_run:
        print("\n=== CONFIGURATION ===")
        for key, value in config.items():
            print(f"{key}: {value}")
        print("\n=== GENERATED ARRAY SCRIPT TEMPLATE ===")
        sample_script = array_template.replace("{JOB_NAME}", job_name).replace("{ARRAY_SIZE}", str(len(commands))).replace("{COMMANDS_FILE}", "/path/to/commands.txt")
        print(sample_script)
        print("=" * 50)
    
    # Submit array job using existing function
    return submit_slurm_array_job(commands, array_template, job_name, dry_run)

def submit_slurm_batch_jobs(commands: List[str], 
                           slurm_script_template: str, 
                           job_name_prefix: str = "batch_job",
                           dry_run: bool = False) -> List[str]:
    """
    Submit multiple commands as separate SLURM batch jobs.
    
    Args:
        commands: List of shell commands to execute
        slurm_script_template: SLURM job script template as string
        job_name_prefix: Prefix for job names (will append index)
        dry_run: If True, only print what would be submitted without actually submitting
    
    Returns:
        List of job IDs (empty strings if dry_run=True or submission fails)
    """
    job_ids = []
    
    for i, command in enumerate(commands):
        # Create unique job name
        job_name = f"{job_name_prefix}_{i+1:03d}"
        
        # Replace placeholders in the template
        job_script = slurm_script_template.format(
            COMMAND=command,
            JOB_NAME=job_name
        )
        
        if dry_run:
            print(f"--- Job {i+1}/{len(commands)}: {job_name} ---")
            print(job_script)
            print("-" * 50)
            job_ids.append("")
            continue
        
        # Create temporary file for the job script
        with tempfile.NamedTemporaryFile(mode='w', suffix='.slurm', delete=False) as f:
            f.write(job_script)
            temp_script_path = f.name
        
        try:
            # Submit the job using sbatch
            result = subprocess.run(
                ['sbatch', temp_script_path],
                capture_output=True,
                text=True,
                check=True
            )
            
            # Extract job ID from sbatch output
            job_id = result.stdout.strip().split()[-1]
            job_ids.append(job_id)
            print(f"Submitted job {job_name} with ID: {job_id}")
            
        except subprocess.CalledProcessError as e:
            print(f"Failed to submit job {job_name}: {e.stderr}")
            job_ids.append("")
            
        except Exception as e:
            print(f"Error submitting job {job_name}: {str(e)}")
            job_ids.append("")
            
        finally:
            # Clean up temporary file
            try:
                os.unlink(temp_script_path)
            except OSError:
                pass
    
    return job_ids

def submit_slurm_array_job(commands: List[str], 
                          slurm_script_template: str,
                          job_name: str = "array_job",
                          dry_run: bool = False) -> Optional[str]:
    """
    Submit commands as a single SLURM array job.
    """
    if not commands:
        print("No commands provided")
        return ""
    
    # Create temporary file with all commands
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        for command in commands:
            f.write(f"{command}\n")
        commands_file = f.name
    
    # Replace placeholders in template
    job_script = slurm_script_template.format(
        JOB_NAME=job_name,
        ARRAY_SIZE=len(commands),
        COMMANDS_FILE=commands_file
    )
    
    if dry_run:
        print(f"--- Array Job: {job_name} ---")
        print(f"Commands file: {commands_file}")
        print("Commands:")
        for i, cmd in enumerate(commands, 1):
            print(f"  {i}: {cmd}")
        print("\nJob script:")
        print(job_script)
        print("-" * 50)
        os.unlink(commands_file)  # Clean up in dry run
        return ""
    
    # Create temporary job script file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.slurm', delete=False) as f:
        f.write(job_script)
        temp_script_path = f.name
    
    try:
        # Submit the array job
        result = subprocess.run(
            ['sbatch', temp_script_path],
            capture_output=True,
            text=True,
            check=True
        )
        
        job_id = result.stdout.strip().split()[-1]
        print(f"Submitted array job {job_name} with ID: {job_id}")
        print(f"Commands file: {commands_file} (keep this file until job completes)")
        return job_id
        
    except subprocess.CalledProcessError as e:
        print(f"Failed to submit array job {job_name}: {e.stderr}")
        return ""
        
    except Exception as e:
        print(f"Error submitting array job {job_name}: {str(e)}")
        return ""
        
    finally:
        # Clean up temporary script file
        try:
            os.unlink(temp_script_path)
        except OSError:
            pass

# Example usage
if __name__ == "__main__":
    # Example commands (your singularity commands)
    example_commands = [
        "module load singularity && singularity run container.sif sub-01",
        "module load singularity && singularity run container.sif sub-02",
        "module load singularity && singularity run container.sif sub-03"
    ]
    
    # Path to your YAML config file
    config_file = "slurm_config.yaml"
    
    # Test with dry run first
    print("=== DRY RUN - Individual Jobs from Config ===")
    submit_jobs_from_config(example_commands, config_file, 
                           job_name_prefix="singularity_job", dry_run=True)
    
    print("\n=== DRY RUN - Array Job from Config ===")
    submit_array_job_from_config(example_commands, config_file,
                                job_name="singularity_array", dry_run=True)
    
    # Uncomment to actually submit jobs:
    # job_ids = submit_jobs_from_config(example_commands, config_file, 
    #                                  job_name_prefix="singularity_job", dry_run=False)
    # 
    # array_job_id = submit_array_job_from_config(example_commands, config_file,
    #                                            job_name="singularity_array", dry_run=False)



# import subprocess


# def gen_slurm_opts(jobqueue_config):
#     slurm_opts = {}
#     slurm_opts['cpus-per-task'] = jobqueue_config['cores']
#     slurm_opts['memory'] = jobqueue_config['memory']
#     slurm_opts['--ntasks'] = 1
#     slurm_opts['queue'] = jobqueue_config['queue']
#     slurm_opts['qos'] = jobqueue_config['qos'],
#     slurm_opts['time'] = jobqueue_config['walltime']
#     slurm_opts['output'] = 'logfile_slurm'
#     slurm_opts['error'] = 'logfile_slurm'


# def run_slurm(cmds: list[str], parallel: bool = False, slurm_opts: dict = None):
#     """
#     Submit each cmd via `sbatch`. parallel flag is ignored (slurm is async).
#     slurm_opts can include qty, mem, etc.
#     """
#     slurm_opts = slurm_opts or {}
#     sbatch_base = ['sbatch']
#     for k, v in slurm_opts.items():
#         sbatch_base += [f'--{k}={v}']

#     job_ids = []
#     for cmd in cmds:
#         full = sbatch_base + ['--wrap', cmd]
#         proc = subprocess.run(full, check=True, capture_output=True, text=True)
#         # parse “Submitted batch job 12345”
#         job_id = proc.stdout.strip().split()[-1]
#         job_ids.append(job_id)
#     return job_ids


