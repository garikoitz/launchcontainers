#!/usr/bin/env python3
import typer
from pathlib import Path
from datetime import datetime
import json
import uuid
from rich.console import Console

app = typer.Typer()
console = Console()


def get_acq_time(json_file: Path):
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


@app.command()
def rename_sbref(
    bids_dir: Path = typer.Option(..., "--bids", "-b"),
    sub: str = typer.Option(..., "--sub", "-s"),
    ses: str = typer.Option(..., "--ses"),
    max_gap: int = typer.Option(60, "--max-gap"),
    dry_run: bool = typer.Option(True, "--dry-run/--execute"),
):
    """Rename sbref files to match magnitude files by time using safe two-phase rename."""
    
    func_dir = bids_dir / f"sub-{sub}" / f"ses-{ses}" / "func"
    
    console.print(f"[cyan]Renaming sbref for sub-{sub} ses-{ses}[/cyan]")
    console.print(f"[yellow]Dry run: {dry_run}[/yellow]\n")
    
    # Get magnitude and sbref files
    mag_files = list(func_dir.glob("*_magnitude.nii.gz"))
    sbref_files = list(func_dir.glob("*_sbref.nii.gz"))
    
    console.print(f"Found {len(mag_files)} magnitude, {len(sbref_files)} sbref\n")
    
    dummy_date = datetime(2000, 1, 1)
    rename_plan = []
    
    for sbref_nii in sbref_files:
        sbref_json = sbref_nii.with_suffix('').with_suffix('.json')
        sbref_time = get_acq_time(sbref_json)
        
        if not sbref_time:
            console.print(f"[red]No time for {sbref_nii.name}[/red]")
            continue
        
        # Find matching magnitude by time
        best_match = None
        min_diff = 999999
        
        for mag_nii in mag_files:
            mag_json = mag_nii.with_suffix('').with_suffix('.json')
            mag_time = get_acq_time(mag_json)
            
            if not mag_time:
                continue
            
            sbref_dt = datetime.combine(dummy_date, sbref_time.time())
            mag_dt = datetime.combine(dummy_date, mag_time.time())
            diff = abs((sbref_dt - mag_dt).total_seconds())
            
            if diff < min_diff and diff <= max_gap:
                min_diff = diff
                best_match = mag_nii
        
        if not best_match:
            console.print(f"[yellow]No match for {sbref_nii.name}[/yellow]")
            continue
        
        # Generate new sbref name from magnitude name
        mag_base = best_match.stem.replace('.nii', '').replace('_magnitude', '')
        new_sbref_base = f"{mag_base}_sbref"
        new_sbref_nii = func_dir / f"{new_sbref_base}.nii.gz"
        new_sbref_json = func_dir / f"{new_sbref_base}.json"
        
        # Check if already correct name
        if sbref_nii == new_sbref_nii:
            console.print(f"[green]Already correct: {sbref_nii.name}[/green]")
            continue
        
        # Generate unique temporary names
        temp_id = uuid.uuid4().hex[:8]
        temp_nii = func_dir / f".tmp_{temp_id}_sbref.nii.gz"
        temp_json = func_dir / f".tmp_{temp_id}_sbref.json"
        
        rename_plan.append({
            'old_nii': sbref_nii,
            'old_json': sbref_json,
            'temp_nii': temp_nii,
            'temp_json': temp_json,
            'new_nii': new_sbref_nii,
            'new_json': new_sbref_json,
            'mag_file': best_match.name,
            'time_diff': int(min_diff)
        })
    
    # Display plan
    console.print(f"\n[cyan]Rename plan ({len(rename_plan)} files):[/cyan]\n")
    for item in rename_plan:
        console.print(f"[yellow]Match (Δt={item['time_diff']}s):[/yellow]")
        console.print(f"  {item['old_nii'].name}")
        console.print(f"  → {item['new_nii'].name}")
        console.print(f"  (based on {item['mag_file']})\n")
    
    # Execute two-phase rename
    if not dry_run:
        console.print("[cyan]Phase 1: Rename to temporary names[/cyan]")
        for item in rename_plan:
            item['old_nii'].rename(item['temp_nii'])
            item['old_json'].rename(item['temp_json'])
            console.print(f"  → {item['temp_nii'].name}")
        
        console.print(f"\n[cyan]Phase 2: Rename to final names[/cyan]")
        for item in rename_plan:
            item['temp_nii'].rename(item['new_nii'])
            item['temp_json'].rename(item['new_json'])
            console.print(f"  [green]✓ {item['new_nii'].name}[/green]")
        
        console.print(f"\n[green]Successfully renamed {len(rename_plan)} sbref pairs[/green]")
    else:
        console.print("[yellow]DRY RUN. Use --execute to rename.[/yellow]")


if __name__ == "__main__":
    app()