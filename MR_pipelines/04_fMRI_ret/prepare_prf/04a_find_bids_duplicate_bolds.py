#!/usr/bin/env python3
import typer
from pathlib import Path
from datetime import datetime
import json
from collections import defaultdict
from rich.console import Console
from rich.table import Table

app = typer.Typer()
console = Console()


def get_acquisition_time(nii_file: Path):
    """Get acquisition time from JSON sidecar."""
    json_file = nii_file.with_suffix('').with_suffix('.json')
    try:
        with open(json_file) as f:
            data = json.load(f)
        acq_time = data.get('AcquisitionTime')
        if acq_time:
            time_str = acq_time.split('.')[0]
            dt = datetime.strptime(f"1900-01-01 {time_str}", '%Y-%m-%d %H:%M:%S')
            return dt
    except:
        pass
    return None


def parse_filename(filename: str):
    """Parse BIDS filename to get components."""
    parts = filename.split('_')
    info = {'sub': None, 'ses': None, 'task': None, 'run': None, 'type': None}
    
    for part in parts:
        if part.startswith('sub-'):
            info['sub'] = part.replace('sub-', '')
        elif part.startswith('ses-'):
            info['ses'] = part.replace('ses-', '')
        elif part.startswith('task-'):
            info['task'] = part.replace('task-', '')
        elif part.startswith('run-'):
            info['run'] = part.replace('run-', '')
    
    if 'sbref' in filename:
        info['type'] = 'sbref'
    elif 'bold' in filename:
        info['type'] = 'bold'
    
    return info


@app.command()
def find_duplicates(
    bids_dir: Path = typer.Option(..., "--bids", "-b"),
):
    """Find duplicate functional runs within each session."""
    
    console.print("[cyan]Scanning for duplicates within sessions...[/cyan]\n")
    
    # Group files by subject/session
    sessions = defaultdict(list)
    
    bids_files = list(bids_dir.glob("sub-*/ses-*/func/*_bold.nii.gz"))
    bids_files += list(bids_dir.glob("sub-*/ses-*/func/*_sbref.nii.gz"))
    
    for f in bids_files:
        acq_time = get_acquisition_time(f)
        if not acq_time:
            continue
        
        info = parse_filename(f.name)
        session_key = f"{info['sub']}/{info['ses']}"
        
        sessions[session_key].append({
            'file': f,
            'time': acq_time,
            'time_str': acq_time.strftime('%H:%M:%S'),
            'task': info['task'],
            'run': info['run'],
            'type': info['type'],
            'info': info
        })
    
    # Find duplicates within each session
    all_duplicates = []
    
    for session_key, files in sessions.items():
        # Group by time within this session
        time_groups = defaultdict(list)
        for item in files:
            time_groups[item['time_str']].append(item)
        
        # Find duplicates (excluding sbref+bold pairs for same task/run)
        for time_str, group in time_groups.items():
            if len(group) < 2:
                continue
            
            # Check if it's just sbref+bold pair for same task/run
            if len(group) == 2:
                if (group[0]['task'] == group[1]['task'] and 
                    group[0]['run'] == group[1]['run'] and
                    {group[0]['type'], group[1]['type']} == {'sbref', 'bold'}):
                    # This is OK - sbref and bold for same task/run
                    continue
            
            # Real duplicate found
            for item in group:
                all_duplicates.append({
                    'session': session_key,
                    'time': time_str,
                    'task': item['task'],
                    'run': item['run'],
                    'type': item['type'],
                    'file': str(item['file'])
                })
    
    if not all_duplicates:
        console.print("\n[green]✅ No duplicates found![/green]")
        return
    
    # Display and save
    console.print(f"\n[red]Found {len(all_duplicates)} duplicate files:[/red]\n")
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Session")
    table.add_column("Time")
    table.add_column("Task")
    table.add_column("Run")
    table.add_column("Type")
    table.add_column("File")
    
    for dup in all_duplicates:
        table.add_row(
            dup['session'],
            dup['time'],
            dup['task'],
            dup['run'],
            dup['type'],
            Path(dup['file']).name
        )
    
    console.print(table)
    
    # Save to file
    with open('duplicates.txt', 'w') as f:
        f.write("session\ttime\ttask\trun\ttype\tfile\n")
        for dup in all_duplicates:
            f.write(f"{dup['session']}\t{dup['time']}\t{dup['task']}\t{dup['run']}\t{dup['type']}\t{dup['file']}\n")
    
    console.print(f"\n[cyan]Saved to duplicates.txt[/cyan]")


if __name__ == "__main__":
    app()