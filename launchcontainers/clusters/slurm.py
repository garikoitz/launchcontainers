
def generate_slurm_script(config, commands_file, num_tasks):
    """Generate SLURM script from configuration."""
    
    job_name = config.get("job_name", "array_job")
    output_dir = config.get("output_dir", ".")
    
    script_lines = [
        "#!/bin/bash",
        f"#SBATCH --job-name={job_name}",
        f"#SBATCH --output={output_dir}/{job_name}_%A_%a.out",
        f"#SBATCH --error={output_dir}/{job_name}_%A_%a.err",
        f"#SBATCH --array=1-{num_tasks}",
    ]
    
    # Add resource specifications
    resource_map = {
        "queue": "--partition",
        "qos": "--qos", 
        "mem": "--mem",
        "cpus": "--cpus-per-task",
        "walltime": "--time"
    }
    
    for key, sbatch_flag in resource_map.items():
        if key in config:
            script_lines.append(f"#SBATCH {sbatch_flag}={config[key]}")
    
    # Add extra directives
    if "extra_directives" in config:
        for directive in config["extra_directives"]:
            script_lines.append(f"#SBATCH {directive}")
    
    script_lines.extend([
        "",
        "# Job execution",
        "set -e  # Exit on error",
        "",
        f"echo \"Starting array task $SLURM_ARRAY_TASK_ID on $(hostname)\"",
        f"echo \"Job ID: $SLURM_JOB_ID\"",
        "",
    ])
    
    # Setup tmpdir if specified
    if "tmpdir" in config:
        script_lines.extend([
            f"export TMPDIR={config['tmpdir']}",
            "mkdir -p $TMPDIR",
            "",
        ])
    
    script_lines.extend([
        "# Get command for this array task",
        f"COMMAND=$(sed -n \"${{SLURM_ARRAY_TASK_ID}}p\" {commands_file})",
        "",
        "echo \"Executing: $COMMAND\"",
        "eval $COMMAND",
        "",
        "echo \"Task $SLURM_ARRAY_TASK_ID completed successfully\"",
    ])
    
    return '\n'.join(script_lines)