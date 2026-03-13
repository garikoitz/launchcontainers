#!/usr/bin/env python3
import typer
from pathlib import Path
from datetime import datetime, timedelta
import scipy.io as sio
import json
import shutil
from rich.console import Console
from rich.table import Table

app = typer.Typer()
console = Console()


def parse_mat_datetime(mat_filename: str):
    """Extract datetime from .mat filename: 20250804T111657.mat"""
    try:
        # Remove .mat extension and parse
        dt_str = mat_filename.replace('.mat', '')
        dt = datetime.strptime(dt_str, '%Y%m%dT%H%M%S')
        adjust_dt = dt - timedelta(minutes=6)
        return adjust_dt
    except: 
        return None


def get_stim_name(mat_file: Path):
    """Extract stimName from .mat file."""
    try:
        mat = sio.loadmat(str(mat_file), simplify_cells=True)
        return mat['params']['loadMatrix']
    except:
        return None


def create_mat_symlinks(vistadisplog: Path, sub: str, ses: str, dry_run: bool = True, force: bool = False):
    """Create symlinks for .mat files based on stimName."""
    
    # Counters for each task type
    counters = {
        'CB': 1, 'FF': 1, 'RW': 1,
        'fixRW': 1, 'fixFF': 1,
        'fixRWblock': 1, 'fixRWblock01': 1, 'fixRWblock02': 1
    }
    
    # Get all .mat files and sort by datetime
    mat_dir = vistadisplog / f"sub-{sub}" / f"ses-{ses}"
    mat_files = sorted(mat_dir.glob("20*.mat"), key=lambda x: x.name)
    
    symlink_map = []
    
    for mat_file in mat_files:
        stim_name = get_stim_name(mat_file)
        if not stim_name:
            console.print(f"[red]Cannot read: {mat_file.name}[/red]")
            continue
        
        mat_dt = parse_mat_datetime(mat_file.name)
        
        # Determine task name based on stimName
        task_name = None
        if 'CB_' in stim_name and 'tr-2' in stim_name:
            task_name = 'retCB'
            run = counters['CB']
            counters['CB'] += 1
        elif 'fixRWblock01_' in stim_name and 'tr-2' in stim_name:
            task_name = 'retfixRWblock01'
            run = counters['fixRWblock01']
            counters['fixRWblock01'] += 1
        elif 'fixRWblock02_' in stim_name and 'tr-2' in stim_name:
            task_name = 'retfixRWblock02'
            run = counters['fixRWblock02']
            counters['fixRWblock02'] += 1
        elif 'fixRWblock_' in stim_name and 'tr-2' in stim_name:
            task_name = 'retfixRWblock'
            run = counters['fixRWblock']
            counters['fixRWblock'] += 1
        elif 'fixFF_' in stim_name and 'tr-2' in stim_name:
            task_name = 'retfixFF'
            run = counters['fixFF']
            counters['fixFF'] += 1
        elif 'fixRW_' in stim_name and 'tr-2' in stim_name:
            task_name = 'retfixRW'
            run = counters['fixRW']
            counters['fixRW'] += 1
        elif 'FF_' in stim_name and 'tr-2' in stim_name:
            task_name = 'retFF'
            run = counters['FF']
            counters['FF'] += 1
        elif 'RW_' in stim_name and 'tr-2' in stim_name:
            task_name = 'retRW'
            run = counters['RW']
            counters['RW'] += 1
        
        if task_name:
            link_name = mat_dir / f"sub-{sub}_ses-{ses}_task-{task_name}_run-{run:02d}_params.mat"
            symlink_map.append({
                'original': mat_file,
                'link': link_name,
                'datetime': mat_dt,
                'task': task_name,
                'run': run,
                'stim_name': stim_name
            })
    
    # Create symlinks
    if not dry_run:
        for item in symlink_map:
            if item['link'].exists() or item['link'].is_symlink():
                if force:
                    item['link'].unlink()
                    item['link'].symlink_to(item['original'].name)
                    console.print(f"[green]Overwritten: {item['link'].name}[/green]")
                else:
                    console.print(f"[yellow]Skipped (exists): {item['link'].name}[/yellow]")
            else:
                item['link'].symlink_to(item['original'].name)
                console.print(f"[green]Created: {item['link'].name}[/green]")
    
    return symlink_map


def get_bids_datetime(json_file: Path):
    """Extract AcquisitionDateTime from BIDS JSON."""

    try:
        with open(json_file) as f:
            data = json.load(f)
        
        # Try AcquisitionTime first (format: "16:05:28.772500")
        acq_time = data.get('AcquisitionTime')
                
        if acq_time:
            # Parse time HH:MM:SS.microseconds
            time_obj = datetime.strptime(acq_time.split('.')[0], '%H:%M:%S')
            # You need acquisition date - might be in another field or filename
            # For now, extract from filename if available
            return time_obj
        
    except:
        pass


def match_bids_files(mat_map, bids_dir: Path, sub: str, ses: str, max_gap: int = 180):
    """Match BIDS files to .mat files by datetime."""
    
    bids_files = list(bids_dir.glob(f"sub-{sub}/ses-{ses}/func/*task-ret*_bold.nii.gz"))
    matches = []
    
    for bids_file in bids_files:
        json_file = bids_file.with_suffix('').with_suffix('.json')
        bids_dt = get_bids_datetime(json_file)
        
        if not bids_dt:
            continue
        
        # Parse BIDS filename
        parts = bids_file.stem.replace('.nii', '').split('_')
        bids_task = bids_run = None
        for part in parts:
            if part.startswith('task-'):
                bids_task = part.replace('task-', '')
            elif part.startswith('run-'):
                bids_run = int(part.replace('run-', ''))
        
        # Find closest .mat match
        best_match = None
        min_diff = timedelta(days=999)
        
        for mat_item in mat_map:
            mat_dt = mat_item['datetime']
            
            mat_time= mat_dt.time()
            bids_time = bids_dt.time()
            dummy_date = datetime(2000, 1, 1)
            mat_time = datetime.combine(dummy_date, mat_time)
            bids_time = datetime.combine(dummy_date, bids_time)
            diff = abs((mat_time - bids_time).total_seconds())
            if diff < min_diff.total_seconds() and diff <= max_gap:
                min_diff = timedelta(seconds=diff)
                best_match = mat_item
        
        if best_match:
            task_match = (bids_task == best_match['task'])
            run_match = (bids_run == best_match['run'])
            
            matches.append({
                'bids_file': bids_file,
                'json_file': json_file,
                'bids_task': bids_task,
                'bids_run': bids_run,
                'mat_task': best_match['task'],
                'mat_run': best_match['run'],
                'time_diff': int(min_diff.total_seconds()),
                'task_match': task_match,
                'run_match': run_match,
                'needs_rename': not (task_match and run_match)
            })
    
    return matches


@app.command()
def link(
    basedir: Path = typer.Option(..., "--basedir", "-b"),
    sub: str = typer.Option(..., "--sub", "-s"),
    ses: str = typer.Option(..., "--ses"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing symlinks"),
    dry_run: bool = typer.Option(True, "--dry-run/--execute"),
):
    """Create symlinks for .mat files based on stimName."""
    
    vistadisplog = basedir / "BIDS" / "sourcedata" / "vistadisplog"
    
    console.print(f"[cyan]Creating .mat symlinks for sub-{sub} ses-{ses}[/cyan]")
    console.print(f"[yellow]Dry run: {dry_run}, Force: {force}[/yellow]\n")
    
    symlink_map = create_mat_symlinks(vistadisplog, sub, ses, dry_run, force)
    
    
    # Display results
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Original File")
    table.add_column("Task")
    table.add_column("Run")
    table.add_column("DateTime")
    
    for item in symlink_map:
        table.add_row(
            item['original'].name,
            item['task'],
            str(item['run']),
            item['datetime'].strftime('%Y-%m-%d %H:%M:%S')
        )
    
    console.print(table)
    
    if dry_run:
        console.print("\n[yellow]This is a DRY RUN. Use --execute to create symlinks.[/yellow]")


@app.command()
def check(
    basedir: Path = typer.Option(..., "--basedir", "-b"),
    sub: str = typer.Option(..., "--sub", "-s"),
    ses: str = typer.Option(..., "--ses"),
    max_gap: int = typer.Option(180, "--max-gap", help="Max time gap in seconds"),
):
    """Check if BIDS files match .mat files by datetime and task name."""
    bids_dir = basedir / "BIDS"
    sourcedata = bids_dir / "sourcedata" / "vistadisplog"
    
    
    console.print(f"[cyan]Checking matches for sub-{sub} ses-{ses}[/cyan]\n")
    
    # Get .mat symlink map
    symlink_map = create_mat_symlinks(sourcedata, sub, ses, dry_run=True)
    console.print("[yellow]MAT Files:[/yellow]")
    for item in symlink_map:
        console.print(f"  {item['original'].name} - {item['datetime'].strftime('%H:%M:%S')} - {item['task']}")
    
    # Get BIDS files with times
    bids_files = list(bids_dir.glob(f"sub-{sub}/ses-{ses}/func/*task-ret*_bold.nii.gz"))
    
    console.print(f"\n[yellow]BIDS Files:[/yellow]")
    bids_list = []
    for bids_file in bids_files:
        json_file = bids_file.with_suffix('').with_suffix('.json')
        bids_dt = get_bids_datetime(json_file)
        
        if bids_dt:
            bids_list.append({
                'file': bids_file,
                'datetime': bids_dt,
                'time_str': bids_dt.strftime('%H:%M:%S')
            })
            console.print(f"  {bids_file.name} - {bids_dt.strftime('%H:%M:%S')}")
        else:
            console.print(f"  {bids_file.name} - [red]NO TIME[/red]")
    
    # Now match them
    console.print(f"\n[cyan]Matching (gap <= {max_gap}s):[/cyan]\n")

    # Match with BIDS
    matches = match_bids_files(symlink_map, bids_dir, sub, ses, max_gap)
    
    # Display results
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("BIDS File")
    table.add_column("BIDS Task")
    table.add_column("BIDS Run")
    table.add_column("MAT Task")
    table.add_column("MAT Run")
    table.add_column("Gap(s)")
    table.add_column("Match")
    
    rename_needed = []
    
    for m in matches:
        match_str = "✓" if not m['needs_rename'] else "✗"
        color = "green" if not m['needs_rename'] else "red"
        
        table.add_row(
            m['bids_file'].name,
            m['bids_task'],
            str(m['bids_run']),
            m['mat_task'],
            str(m['mat_run']),
            str(m['time_diff']),
            f"[{color}]{match_str}[/{color}]"
        )
        
        if m['needs_rename']:
            rename_needed.append(m)
    
    console.print(table)
    
    # Show rename plan
    if rename_needed:
        console.print(f"\n[red]Found {len(rename_needed)} files that need renaming:[/red]\n")
        
        for m in rename_needed:
            old_name = m['bids_file'].name
            new_name = old_name.replace(
                f"task-{m['bids_task']}_run-{m['bids_run']:02d}",
                f"task-{m['mat_task']}_run-{m['mat_run']:02d}"
            )
            console.print(f"  {old_name}")
            console.print(f"  → {new_name}\n")
        
        console.print("[yellow]Use 'rename' command to apply changes[/yellow]")


@app.command()
def rename(
    basedir: Path = typer.Option(..., "--basedir", "-b"),
    sub: str = typer.Option(..., "--sub", "-s"),
    ses: str = typer.Option(..., "--ses"),
    max_gap: int = typer.Option(120, "--max-gap"),
    dry_run: bool = typer.Option(True, "--dry-run/--execute"),
):
    """Rename BIDS files to match .mat task names."""
    bids_dir = basedir / "BIDS"
    sourcedata = bids_dir / "sourcedata" / 'vistadisplog'
    
    
    console.print(f"[cyan]Renaming files for sub-{sub} ses-{ses}[/cyan]")
    console.print(f"[yellow]Dry run: {dry_run}[/yellow]\n")
    
    # Get matches
    symlink_map = create_mat_symlinks(sourcedata, sub, ses, dry_run=True)
    matches = match_bids_files(symlink_map, bids_dir, sub, ses, max_gap)
    
    rename_count = 0
    
    for m in matches:
        if not m['needs_rename']:
            continue
        
        old_nii = m['bids_file']
        old_json = m['json_file']
        
        # Generate new names
        old_base = old_nii.stem.replace('.nii', '')
        new_base = old_base.replace(
            f"task-{m['bids_task']}_run-{m['bids_run']:02d}",
            f"task-{m['mat_task']}_run-{m['mat_run']:02d}"
        )
        
        new_nii = old_nii.parent / f"{new_base}.nii.gz"
        new_json = old_json.parent / f"{new_base}.json"
        
        console.print(f"[yellow]Renaming:[/yellow]")
        console.print(f"  {old_nii.name} → {new_nii.name}")
        console.print(f"  {old_json.name} → {new_json.name}")
        
        if not dry_run:
            old_nii.rename(new_nii)
            old_json.rename(new_json)
            console.print("[green]  ✓ Done[/green]\n")
            rename_count += 1
        else:
            console.print()
    
    if dry_run:
        console.print("\n[yellow]This is a DRY RUN. Use --execute to rename files.[/yellow]")
    else:
        console.print(f"\n[green]Renamed {rename_count} file pairs[/green]")


if __name__ == "__main__":
    app()