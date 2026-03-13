#!/usr/bin/env python3
import typer
from pathlib import Path
from datetime import datetime, timedelta
import scipy.io as sio
import json
import pandas as pd
from rich.console import Console
from rich.table import Table

app = typer.Typer()
console = Console()


def parse_mat_datetime(mat_filename: str):
    """Extract datetime from .mat filename and adjust -6min."""
    try:
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


def get_bids_datetime(json_file: Path):
    """Extract AcquisitionTime from JSON."""
    try:
        with open(json_file) as f:
            data = json.load(f)
        acq_time = data.get('AcquisitionTime')
        if acq_time:
            time_obj = datetime.strptime(acq_time.split('.')[0], '%H:%M:%S')
            return time_obj
    except:
        pass
    return None


def parse_scans_tsv(scans_file: Path):
    """Parse scans.tsv file to get magnitude files (treated as bold)."""
    try:
        df = pd.read_csv(scans_file, sep='\t')
        
        # Filter for magnitude files (will be converted to bold)
        mag_files = df[df['filename'].str.contains('magnitude')].copy()
        
        results = []
        for _, row in mag_files.iterrows():
            filename = row['filename']
            acq_time = row['acq_time']
            
            # Parse filename: func/sub-01_ses-10_task-retRW_run-01_magnitude.nii.gz
            parts = Path(filename).stem.replace('.nii', '').split('_')
            task = run = None
            for part in parts:
                if part.startswith('task-'):
                    task = part.replace('task-', '')
                elif part.startswith('run-'):
                    run = int(part.replace('run-', ''))
            
            # Parse datetime
            dt = datetime.fromisoformat(acq_time.replace('Z', '+00:00'))
            
            results.append({
                'filename': filename,
                'task': task,
                'run': run,
                'datetime': dt,
                'time_str': dt.strftime('%H:%M:%S')
            })
        
        return results
    except Exception as e:
        console.print(f"[red]Error reading scans.tsv: {e}[/red]")
        return []


@app.command()
def create_mapping(
    bids_dir: Path = typer.Option(..., "--bids", "-b"),
    sub: str = typer.Option(..., "--sub", "-s"),
    ses: str = typer.Option(..., "--ses"),
    max_gap: int = typer.Option(180, "--max-gap"),
    output: Path = typer.Option("mapping.tsv", "--output", "-o"),
):
    """Create comprehensive mapping from .mat to BIDS to fMRIPrep."""
    
    console.print(f"[cyan]Creating mapping for sub-{sub} ses-{ses}[/cyan]\n")
    
    vistadisplog = bids_dir / "sourcedata" / "vistadisplog"
    scans_file = bids_dir / f"sub-{sub}" / f"ses-{ses}" / f"sub-{sub}_ses-{ses}_scans.tsv"
    
    # Step 1: Get .mat files with task names, run numbers, and times
    mat_dir = vistadisplog / f"sub-{sub}" / f"ses-{ses}"
    mat_files = sorted(mat_dir.glob("20*.mat"), key=lambda x: x.name)
    
    # Counters for each task type (same logic as link function)
    counters = {
        'CB': 1, 'FF': 1, 'RW': 1,
        'fixRW': 1, 'fixFF': 1,
        'fixRWblock': 1, 'fixRWblock01': 1, 'fixRWblock02': 1
    }
    
    console.print("[yellow]Step 1: Reading .mat files[/yellow]")
    mat_data = []
    
    for mat_file in mat_files:
        stim_name = get_stim_name(mat_file)
        if not stim_name or 'tr-2' not in stim_name:
            continue
        
        mat_dt = parse_mat_datetime(mat_file.name)
        
        # Decode task name and increment run counter
        task_name = None
        run_num = None
        
        if 'CB_' in stim_name and 'tr-2' in stim_name:
            task_name = 'retCB'
            run_num = counters['CB']
            counters['CB'] += 1
        elif 'fixRWblock01_' in stim_name and 'tr-2' in stim_name:
            task_name = 'retfixRWblock01'
            run_num = counters['fixRWblock01']
            counters['fixRWblock01'] += 1
        elif 'fixRWblock02_' in stim_name and 'tr-2' in stim_name:
            task_name = 'retfixRWblock02'
            run_num = counters['fixRWblock02']
            counters['fixRWblock02'] += 1
        elif 'fixRWblock_' in stim_name and 'tr-2' in stim_name:
            task_name = 'retfixRWblock'
            run_num = counters['fixRWblock']
            counters['fixRWblock'] += 1
        elif 'fixFF_' in stim_name and 'tr-2' in stim_name:
            task_name = 'retfixFF'
            run_num = counters['fixFF']
            counters['fixFF'] += 1
        elif 'fixRW_' in stim_name and 'tr-2' in stim_name:
            task_name = 'retfixRW'
            run_num = counters['fixRW']
            counters['fixRW'] += 1
        elif 'FF_' in stim_name and 'tr-2' in stim_name:
            task_name = 'retFF'
            run_num = counters['FF']
            counters['FF'] += 1
        elif 'RW_' in stim_name and 'tr-2' in stim_name:
            task_name = 'retRW'
            run_num = counters['RW']
            counters['RW'] += 1
        
        if task_name and run_num:
            mat_data.append({
                'source': 'mat',
                'file': mat_file.name,
                'task': task_name,
                'run': run_num,
                'datetime': mat_dt,
                'time_str': mat_dt.strftime('%H:%M:%S')
            })
    
    console.print(f"  Found {len(mat_data)} .mat files")
    
    # Step 2: Get BIDS bold files
    console.print("[yellow]Step 2: Reading BIDS bold files[/yellow]")
    bold_files = list(bids_dir.glob(f"sub-{sub}/ses-{ses}/func/*task-ret*_bold.nii.gz"))
    
    bids_data = []
    for bold_file in bold_files:
        json_file = bold_file.with_suffix('').with_suffix('.json')
        bids_dt = get_bids_datetime(json_file)
        
        if not bids_dt:
            continue
        
        parts = bold_file.stem.replace('.nii', '').split('_')
        task = run = None
        for part in parts:
            if part.startswith('task-'):
                task = part.replace('task-', '')
            elif part.startswith('run-'):
                run = int(part.replace('run-', ''))
        
        bids_data.append({
            'source': 'bids',
            'file': bold_file.name,
            'task': task,
            'run': run,
            'datetime': bids_dt,
            'time_str': bids_dt.strftime('%H:%M:%S')
        })
    
    console.print(f"  Found {len(bids_data)} bold files")
    
    # Step 3: Get scans.tsv data
    console.print("[yellow]Step 3: Reading scans.tsv[/yellow]")
    scans_data = []
    if scans_file.exists():
        scans_data = parse_scans_tsv(scans_file)
        console.print(f"  Found {len(scans_data)} magnitude files")
    else:
        console.print(f"  [red]scans.tsv not found[/red]")
    
    # Step 4: Create mapping by time matching
    console.print(f"[yellow]Step 4: Creating mapping (max gap: {max_gap}s)[/yellow]")
    
    dummy_date = datetime(2000, 1, 1)
    mapping = []
    
    for mat_item in mat_data:
        mat_time = datetime.combine(dummy_date, mat_item['datetime'].time())
        
        # Find matching BIDS bold
        best_bids = None
        min_bids_diff = timedelta(days=999)
        
        for bids_item in bids_data:
            bids_time = datetime.combine(dummy_date, bids_item['datetime'].time())
            diff = abs((mat_time - bids_time).total_seconds())
            
            if diff < min_bids_diff.total_seconds() and diff <= max_gap:
                min_bids_diff = timedelta(seconds=diff)
                best_bids = bids_item
        
        # Find matching scans.tsv magnitude
        best_scans = None
        min_scans_diff = timedelta(days=999)
        
        for scans_item in scans_data:
            scans_time = datetime.combine(dummy_date, scans_item['datetime'].time())
            diff = abs((mat_time - scans_time).total_seconds())
            
            if diff < min_scans_diff.total_seconds() and diff <= max_gap:
                min_scans_diff = timedelta(seconds=diff)
                best_scans = scans_item
        
        mapping.append({
            'mat_file': mat_item['file'],
            'mat_task': mat_item['task'],
            'mat_run': f"{mat_item['run']:02d}",
            'mat_time': mat_item['time_str'],
            'bids_file': best_bids['file'] if best_bids else 'NO_MATCH',
            'bids_task': best_bids['task'] if best_bids else '-',
            'bids_run': f"{best_bids['run']:02d}" if best_bids else '-',
            'bids_time': best_bids['time_str'] if best_bids else '-',
            'bids_time_diff': int(min_bids_diff.total_seconds()) if best_bids else '-',
            'scans_file': best_scans['filename'] if best_scans else 'NO_MATCH',
            'scans_task': best_scans['task'] if best_scans else '-',
            'scans_run':  f"{best_scans['run']:02d}" if best_scans else '-',
            'scans_time': best_scans['time_str'] if best_scans else '-',
            'scans_time_diff': int(min_scans_diff.total_seconds()) if best_scans else '-',
        })
    
    # Display mapping
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("MAT Task")
    table.add_column("MAT Run")
    table.add_column("MAT Time")
    table.add_column("BIDS Task")
    table.add_column("BIDS Run")
    table.add_column("Δt(s)")
    table.add_column("Scans Task")
    table.add_column("Scans Run")
    table.add_column("Δt(s)")
    
    for m in mapping:
        bids_color = "green" if m['bids_task'] != '-' else "red"
        scans_color = "green" if m['scans_task'] != '-' else "red"
        
        table.add_row(
            m['mat_task'],
            str(m['mat_run']),
            m['mat_time'],
            f"[{bids_color}]{m['bids_task']}[/{bids_color}]",
            str(m['bids_run']),
            str(m['bids_time_diff']),
            f"[{scans_color}]{m['scans_task']}[/{scans_color}]",
            str(m['scans_run']),
            str(m['scans_time_diff']),
        )
    
    console.print(table)
    
    # Save to TSV
    df = pd.DataFrame(mapping)
    df.to_csv(output, sep='\t', index=False)
    
    console.print(f"\n[cyan]Mapping saved to {output}[/cyan]")
    console.print(f"[cyan]Use this to understand: MAT task → BIDS task (before rename)[/cyan]")


if __name__ == "__main__":
    app()

