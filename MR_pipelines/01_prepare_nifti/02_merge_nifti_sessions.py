#!/usr/bin/env python3
import typer
from pathlib import Path
from datetime import datetime
import json
import shutil
import pandas as pd
from collections import defaultdict
from rich.console import Console
from rich.table import Table
from typing import List, Optional
import re

app = typer.Typer()
console = Console()


def get_acq_time_from_json(json_file: Path):
    """Get acquisition time from JSON."""
    try:
        with open(json_file) as f:
            data = json.load(f)
        acq_time = data.get('AcquisitionTime')
        if acq_time:
            return datetime.strptime(acq_time.split('.')[0], '%H:%M:%S')
    except:
        pass
    return None


def parse_bids_filename(filename: str):
    """Parse BIDS filename to extract entities."""
    parts = filename.split('_')
    entities = {}
    
    for part in parts:
        if '-' in part:
            key, val = part.split('-', 1)
            entities[key] = val
    
    # Get modality (last part without extension)
    stem = filename.replace('.nii.gz', '').replace('.json', '').replace('.tsv', '').replace('.bval', '').replace('.bvec', '')
    modality = stem.split('_')[-1]
    entities['modality'] = modality
    
    return entities


def get_max_run(files, task=None):
    """Get maximum run number for a given task (or all if task=None)."""
    max_run = 0
    
    for f in files:
        entities = parse_bids_filename(f.name)
        
        if task and entities.get('task') != task:
            continue
        
        run_str = entities.get('run')
        if run_str:
            try:
                run_num = int(run_str)
                max_run = max(max_run, run_num)
            except:
                pass
    
    return max_run


def format_run_number(run_num: int, modality: str) -> str:
    """Format run number based on modality."""
    if modality == 'fmap':
        return f"{run_num:01d}"  # Single digit for fmap
    else:
        return f"{run_num:02d}"  # Two digits for other modalities


def get_modality_from_filename(filename: str) -> str:
    """Extract modality from BIDS filename path."""
    if '/' in filename:
        return filename.split('/')[0]
    return 'unknown'


def update_scans_tsv_filename(old_filename: str, ses_second: str, ses_first: str, 
                              rename_plan: list, max_runs: dict, all_modalities: list) -> str:
    """
    Update a filename in scans.tsv with proper session and run number renaming.
    
    Args:
        old_filename: Original filename from scans.tsv
        ses_second: Second session ID to replace
        ses_first: First session ID to use
        rename_plan: List of rename plan items for files being moved
        max_runs: Dict of max run numbers per modality/task
        all_modalities: List of all modalities being processed
    
    Returns:
        Updated filename
    """
    # Extract the actual filename (without path)
    if '/' in old_filename:
        path_parts = old_filename.split('/')
        modality = path_parts[0]
        basename = path_parts[1]
    else:
        modality = 'unknown'
        basename = old_filename
    
    # First, check if this file is in the rename plan (being physically moved)
    for plan_item in rename_plan:
        if plan_item['old_file'].name == basename:
            # Use the new name from the plan
            return f"{modality}/{plan_item['new_name']}"
    
    # If not in rename plan, still need to update session ID and potentially run number
    new_filename = basename.replace(f"ses-{ses_second}", f"ses-{ses_first}")
    
    # Check if this file needs run number update (for modalities being merged)
    entities = parse_bids_filename(basename)
    file_modality = get_modality_from_filename(old_filename)
    
    # Only update run numbers if this modality is being merged
    if file_modality in all_modalities:
        old_run = entities.get('run')
        
        if old_run:
            old_run_num = int(old_run)
            
            # Determine offset based on modality
            if file_modality == 'func':
                task = entities.get('task')
                offset = max_runs.get(f"func_{task}", 0)
            else:
                offset = max_runs.get(file_modality, 0)
            
            if offset > 0:
                new_run_num = old_run_num + offset
                
                # Format run number based on modality
                old_run_str = format_run_number(old_run_num, file_modality)
                new_run_str = format_run_number(new_run_num, file_modality)
                
                new_filename = new_filename.replace(f"run-{old_run_str}", f"run-{new_run_str}")
    
    return f"{modality}/{new_filename}"


@app.command()
def merge_sessions(
    bids_dir: Path = typer.Option(..., "--bids", "-b"),
    sub: str = typer.Option(..., "--sub", "-s"),
    ses_first: str = typer.Option(..., "--ses-first"),
    ses_second: str = typer.Option(..., "--ses-second"),
    modalities: Optional[List[str]] = typer.Option(
        None, "--modality", "-m",
        help="Modalities to merge (can specify multiple: -m func -m anat). If not specified, merges all."
    ),
    log_dir: Path = typer.Option("log_merge_nifti", "--log-dir", "-l"),
    dry_run: bool = typer.Option(True, "--dry-run/--execute"),
):
    """
    Merge two session halves into one session.
    
    Examples:
        # Merge all modalities (dry run)
        merge_sessions.py --bids /path/to/bids --sub 01 --ses-first 02 --ses-second 02part1
        
        # Merge only func data (execute)
        merge_sessions.py --bids /path/to/bids --sub 01 --ses-first 02 --ses-second 02part1 -m func --execute
        
        # Merge func and anat (execute)
        merge_sessions.py --bids /path/to/bids --sub 01 --ses-first 02 --ses-second 02part1 -m func -m anat --execute
    """

    if not log_dir.is_absolute():
        log_dir = bids_dir / log_dir

    # Default to all modalities if none specified
    all_modalities = ['func', 'anat', 'fmap', 'dwi']
    if modalities is None or len(modalities) == 0:
        selected_modalities = all_modalities
    else:
        # Validate modalities
        invalid = set(modalities) - set(all_modalities)
        if invalid:
            console.print(f"[red]Error: Invalid modalities: {', '.join(invalid)}[/red]")
            console.print(f"[yellow]Valid modalities: {', '.join(all_modalities)}[/yellow]")
            return
        selected_modalities = modalities

    # Create log directory
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    mod_suffix = "_".join(selected_modalities) if len(selected_modalities) < len(all_modalities) else "all"
    log_file = log_dir / f"merge_sub-{sub}_ses-{ses_first}+{ses_second}_{mod_suffix}_{timestamp}.log"
    
    # Open log file
    log = open(log_file, 'w')
    
    def log_print(msg, color=None):
        """Print to console and log file."""
        # Strip color codes for log file
        clean_msg = msg
        for code in ['[cyan]', '[/cyan]', '[yellow]', '[/yellow]', '[green]', '[/green]', '[red]', '[/red]', '[bold magenta]', '[/bold magenta]']:
            clean_msg = clean_msg.replace(code, '')
        log.write(clean_msg + '\n')
        log.flush()
        
        if color:
            console.print(f"[{color}]{msg}[/{color}]")
        else:
            console.print(msg)
    
    log_print(f"Session Merge Log - {timestamp}")
    log_print(f"Subject: sub-{sub}")
    log_print(f"First half: ses-{ses_first}")
    log_print(f"Second half: ses-{ses_second}")
    log_print(f"Modalities to merge: {', '.join(selected_modalities)}")
    log_print(f"Dry run: {dry_run}")
    log_print("=" * 80 + "\n")
    
    console.print(f"[cyan]Merging sessions for sub-{sub}[/cyan]")
    console.print(f"  First half: ses-{ses_first}")
    console.print(f"  Second half: ses-{ses_second}")
    console.print(f"  Modalities: {', '.join(selected_modalities)}")
    console.print(f"[yellow]Dry run: {dry_run}[/yellow]")
    console.print(f"[cyan]Log file: {log_file}[/cyan]\n")
    
    first_dir = bids_dir / f"sub-{sub}" / f"ses-{ses_first}"
    second_dir = bids_dir / f"sub-{sub}" / f"ses-{ses_second}"
    
    if not first_dir.exists():
        log_print(f"Error: {first_dir} does not exist", "red")
        log.close()
        return
    
    if not second_dir.exists():
        log_print(f"Error: {second_dir} does not exist", "red")
        log.close()
        return
    
    # Step 1: Analyze first session to get max run numbers per task
    log_print("Step 1: Analyzing first session", "yellow")
    
    first_files = {}
    max_runs = {}
    
    for modality in selected_modalities:
        mod_dir = first_dir / modality
        if mod_dir.exists():
            files = list(mod_dir.glob("*.nii.gz"))
            first_files[modality] = files
            
            # Get max run per task for func
            if modality == 'func':
                tasks = set()
                for f in files:
                    entities = parse_bids_filename(f.name)
                    task = entities.get('task')
                    if task:
                        tasks.add(task)
                
                for task in tasks:
                    max_run = get_max_run(files, task=task)
                    max_runs[f"{modality}_{task}"] = max_run
                    log_print(f"  func task-{task}: max run = {max_run}")
            else:
                max_run = get_max_run(files)
                if max_run > 0:
                    max_runs[modality] = max_run
                    log_print(f"  {modality}: max run = {max_run}")
    
    # Step 2: Read scans.tsv to verify order by time
    log_print("\nStep 2: Verifying session order by acquisition time", "yellow")
    
    first_scans = first_dir / f"sub-{sub}_ses-{ses_first}_scans.tsv"
    second_scans = second_dir / f"sub-{sub}_ses-{ses_second}_scans.tsv"
    
    # Create backups of scans.tsv files
    if not dry_run:
        backup_dir = log_dir / "backups"
        backup_dir.mkdir(exist_ok=True)
        
        if first_scans.exists():
            backup_first = backup_dir / f"sub-{sub}_ses-{ses_first}_scans_{timestamp}.tsv"
            shutil.copy2(first_scans, backup_first)
            log_print(f"  Created backup: {backup_first}")
        
        if second_scans.exists():
            backup_second = backup_dir / f"sub-{sub}_ses-{ses_second}_scans_{timestamp}.tsv"
            shutil.copy2(second_scans, backup_second)
            log_print(f"  Created backup: {backup_second}")
    
    if first_scans.exists() and second_scans.exists():
        df1 = pd.read_csv(first_scans, sep='\t')
        df2 = pd.read_csv(second_scans, sep='\t')
        
        last_time_first = pd.to_datetime(df1['acq_time'].max())
        first_time_second = pd.to_datetime(df2['acq_time'].min())
        
        log_print(f"  First session last scan: {last_time_first}")
        log_print(f"  Second session first scan: {first_time_second}")
        
        if first_time_second < last_time_first:
            log_print("  WARNING: Second session starts BEFORE first session ends!", "red")
            log_print("  Are you sure about the order?", "red")
        else:
            log_print("  ✓ Order verified", "green")
    
    # Step 3: Plan renames for second session
    log_print("\nStep 3: Planning renames for second session", "yellow")
    
    rename_plan = []
    
    for modality in selected_modalities:
        mod_dir = second_dir / modality
        if not mod_dir.exists():
            log_print(f"  Skipping {modality}: not found in second session", "yellow")
            continue
        
        # Collect all files including .bval and .bvec for DWI
        files = list(mod_dir.glob("*.nii.gz")) + list(mod_dir.glob("*.json"))
        if modality == 'dwi':
            files += list(mod_dir.glob("*.bval")) + list(mod_dir.glob("*.bvec"))
        
        log_print(f"  Processing {modality}: {len(files)} files")
        
        for f in files:
            entities = parse_bids_filename(f.name)
            old_run = entities.get('run')
            
            if not old_run:
                # No run number, just needs session rename
                new_name = f.name.replace(f"ses-{ses_second}", f"ses-{ses_first}")
                rename_plan.append({
                    'old_file': f,
                    'new_name': new_name,
                    'modality': modality,
                    'run_offset': 0
                })
            else:
                # Has run number - need to offset
                old_run_num = int(old_run)
                
                # Determine offset
                if modality == 'func':
                    task = entities.get('task')
                    offset = max_runs.get(f"{modality}_{task}", 0)
                else:
                    offset = max_runs.get(modality, 0)
                
                new_run_num = old_run_num + offset
                
                # Generate new filename with proper formatting based on modality
                new_name = f.name.replace(f"ses-{ses_second}", f"ses-{ses_first}")
                
                # Format run number based on modality
                old_run_str = format_run_number(old_run_num, modality)
                new_run_str = format_run_number(new_run_num, modality)
                
                new_name = new_name.replace(f"run-{old_run_str}", f"run-{new_run_str}")
                
                rename_plan.append({
                    'old_file': f,
                    'new_name': new_name,
                    'modality': modality,
                    'old_run': old_run_num,
                    'new_run': new_run_num,
                    'run_offset': offset
                })
    
    # Display rename plan
    log_print(f"\nRename plan ({len(rename_plan)} files):\n")
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Modality")
    table.add_column("Old Name")
    table.add_column("New Name")
    table.add_column("Run Change")
    
    for item in rename_plan:
        old_name = item['old_file'].name
        new_name = item['new_name']
        
        if 'old_run' in item:
            run_change = f"{item['old_run']} → {item['new_run']} (+{item['run_offset']})"
        else:
            run_change = "—"
        
        table.add_row(item['modality'], old_name, new_name, run_change)
        
        # Also log to file
        log.write(f"{item['modality']:<10} {old_name:<80} → {new_name:<80} {run_change}\n")
    
    console.print(table)
    log.write("\n")
    
    # Step 4: Merge scans.tsv with intelligent renaming
    log_print("\nStep 4: Planning scans.tsv merge", "yellow")
    log_print("  Note: scans.tsv will be updated for ALL files with proper run number offsetting", "cyan")
    
    merged_scans = None
    if first_scans.exists() and second_scans.exists():
        df1 = pd.read_csv(first_scans, sep='\t')
        df2 = pd.read_csv(second_scans, sep='\t')
        
        # Update second session filenames in scans.tsv using intelligent logic
        df2_updated = df2.copy()
        for idx, row in df2_updated.iterrows():
            old_filename = row['filename']
            
            # Use the smart update function
            new_filename = update_scans_tsv_filename(
                old_filename, 
                ses_second, 
                ses_first, 
                rename_plan, 
                max_runs,
                selected_modalities
            )
            
            df2_updated.at[idx, 'filename'] = new_filename
            
            # Log the change if it's different
            if old_filename != new_filename:
                log.write(f"  scans.tsv: {old_filename} → {new_filename}\n")
        
        # Merge
        merged_scans = pd.concat([df1, df2_updated], ignore_index=True)
        merged_scans = merged_scans.sort_values('acq_time')
        
        # Remove duplicates (same filename appearing in both sessions)
        initial_count = len(merged_scans)
        merged_scans = merged_scans.drop_duplicates(subset=['filename'], keep='first')
        duplicates_removed = initial_count - len(merged_scans)
        
        log_print(f"  First session: {len(df1)} scans")
        log_print(f"  Second session: {len(df2)} scans")
        log_print(f"  Merged: {len(merged_scans)} scans")
        if duplicates_removed > 0:
            log_print(f"  Duplicates removed: {duplicates_removed}", "yellow")
        log_print(f"  Files physically moved: {len(rename_plan)}")
    
    # Execute if not dry run
    if not dry_run:
        log_print("\nExecuting merge...", "cyan")
        
        # Rename and move files
        if rename_plan:
            log_print(f"Moving and renaming {len(rename_plan)} files...", "yellow")
            for item in rename_plan:
                old_file = item['old_file']
                new_file = first_dir / item['modality'] / item['new_name']
                
                # Ensure target directory exists
                new_file.parent.mkdir(parents=True, exist_ok=True)
                
                # Move file
                shutil.move(str(old_file), str(new_file))
                log_print(f"  ✓ {item['modality']}/{item['new_name']}", "green")
        else:
            log_print("  No files to move", "yellow")
        
        # Merge scans.tsv (ALWAYS done)
        if merged_scans is not None:
            log_print("Merging scans.tsv...", "yellow")
            merged_scans.to_csv(first_scans, sep='\t', index=False)
            log_print(f"  ✓ Updated {first_scans.name}", "green")
        
        # Remove second session modality directories if empty
        try:
            for modality in selected_modalities:
                mod_dir = second_dir / modality
                if mod_dir.exists() and not any(mod_dir.iterdir()):
                    mod_dir.rmdir()
                    log_print(f"  Removed empty {modality}/ directory", "green")
            
            # Remove second session directory if empty
            if not any(second_dir.iterdir()):
                second_dir.rmdir()
                log_print(f"\n✓ Removed empty ses-{ses_second} directory", "green")
            else:
                remaining = [d.name for d in second_dir.iterdir() if d.is_dir()]
                log_print(f"\nNote: ses-{ses_second} still contains: {', '.join(remaining)}", "yellow")
        except Exception as e:
            log_print(f"\nNote: Could not remove directories: {e}", "yellow")
        
        log_print("\n✓ Sessions merged successfully!", "green")
        log_print(f"  Modalities merged: {', '.join(selected_modalities)}", "green")
        log_print(f"  Files moved: {len(rename_plan)}", "green")
    else:
        log_print("\nDRY RUN complete. Use --execute to merge.", "yellow")
    
    log.close()
    console.print(f"\n[cyan]Log saved to: {log_file}[/cyan]")


if __name__ == "__main__":
    app()
