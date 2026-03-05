#!/usr/bin/env python3
"""
Clean Extra SBRef Files After Heudiconv Conversion
===================================================

This script addresses a specific issue with heudiconv workflows when MRI scans
are interrupted mid-acquisition and then restarted.

Problem:
--------
When a functional run is stopped partway through (e.g., only 50 of 160 volumes acquired)
and then restarted, the following happens:

1. First attempt (aborted):
   - Scanner creates run-01 SBRef (1 volume) → heudiconv converts to run-01_sbref
   - Scanner starts run-01 bold but scan is stopped → heudiconv REJECTS (< 160 volumes)

2. Scan is restarted:
   - Scanner creates run-02 SBRef (1 volume) → heudiconv converts to run-02_sbref
   - Scanner completes run-02 bold (160 volumes) → heudiconv converts to run-02_bold

3. Heuristic file corrects run numbers:
   - run-02_sbref → run-01_sbref (matches protocol run number)
   - run-02_bold → run-01_bold (matches protocol run number)

Result After Heudiconv:
-----------------------
    sub-01/ses-01/func/
    ├── sub-01_ses-01_task-fLoc_run-01_sbref.nii.gz   # From aborted attempt (EXTRA!)
    ├── sub-01_ses-01_task-fLoc_run-01_sbref.json
    ├── sub-01_ses-01_task-fLoc_run-01_sbref.nii.gz   # From successful retry (KEEP!)
    ├── sub-01_ses-01_task-fLoc_run-01_sbref.json
    ├── sub-01_ses-01_task-fLoc_run-01_bold.nii.gz    # From successful retry
    └── sub-01_ses-01_task-fLoc_run-01_bold.json

The Issue:
----------
You end up with TWO run-01 SBRef files (with identical filenames, causing BIDS errors):
- One from the aborted first attempt (scanner run-01)
- One from the successful retry (scanner run-02, renamed to run-01 by heuristic)

But only ONE run-01 bold file (from the successful retry).

The heuristic file's `extract_run_number()` function reads the run number from the 
protocol name and renames both scanner run-02 files to run-01, but it can't remove 
the orphaned run-01 SBRef from the failed first attempt.

Solution:
---------
This script identifies which SBRef to keep by comparing acquisition times:
1. Groups files by (subject, session, task, run) 
2. For each run with multiple SBRefs, finds which SBRef was acquired closest in time
   to the bold file
3. The SBRef from the successful attempt (run-02 → run-01) will match the bold time
4. The SBRef from the aborted attempt (original run-01) will be much earlier
5. Removes the extra SBRef that doesn't temporally match the bold

Note on Run Numbering:
---------------------
The run numbers you see in BIDS are the PROTOCOL run numbers (from heuristic file),
not the scanner's sequential series numbers. When a run is restarted:
- Scanner increments series number: run-01 → run-02
- Heuristic corrects back to protocol run: run-02 → run-01 (via extract_run_number)
- But the orphaned run-01 SBRef from the failed attempt remains

Usage:
------
# Step 1: Scan your BIDS dataset and identify extra SBRefs
python clean_extra_sbrefs.py find-extra-sbrefs \\
    --bids /path/to/bids \\
    --output extra_sbrefs_to_remove.txt

# Step 2: Review the list and preview what will be removed
python clean_extra_sbrefs.py remove-extra-sbrefs \\
    --input extra_sbrefs_to_remove.txt \\
    --dry-run

# Step 3: Execute the removal
python clean_extra_sbrefs.py remove-extra-sbrefs \\
    --input extra_sbrefs_to_remove.txt \\
    --execute

Example Scenario:
----------------
Timeline of events:

10:00 AM - Scanner creates run-01 SBRef
10:01 AM - Scanner starts run-01 bold
10:02 AM - Scan aborted (only 50/160 volumes)
10:05 AM - Scanner creates run-02 SBRef  
10:06 AM - Scanner completes run-02 bold (160 volumes)

After heudiconv with run number correction:
    sub-01/ses-01/func/
    ├── ...run-01_sbref.nii.gz    # 10:00 AM - EXTRA (from aborted run-01)
    ├── ...run-01_sbref.nii.gz    # 10:05 AM - KEEP (from run-02, renamed to run-01)
    └── ...run-01_bold.nii.gz     # 10:06 AM - matches 10:05 AM sbref

This script identifies that the 10:05 AM SBRef is closest to the 10:06 AM bold,
and removes the 10:00 AM SBRef from the failed attempt.

After cleanup:
    sub-01/ses-01/func/
    ├── sub-01_ses-01_task-fLoc_run-01_sbref.nii.gz    # 10:05 AM
    ├── sub-01_ses-01_task-fLoc_run-01_sbref.json
    ├── sub-01_ses-01_task-fLoc_run-01_bold.nii.gz     # 10:06 AM
    └── sub-01_ses-01_task-fLoc_run-01_bold.json

Notes:
------
- Acquisition time is read from the 'AcquisitionTime' field in JSON sidecars
- Both .nii.gz and .json files are removed together
- Always run with --dry-run first to review what will be removed
- The script operates within sessions (doesn't compare across sessions)
- The "extra" SBRef is typically several minutes earlier than the valid one

Why Not Just Match by Filename?
-------------------------------
Because both SBRefs have IDENTICAL filenames after heudiconv's run number correction.
We can only distinguish them by their acquisition timestamps.

Author: Tiger Lei
Date: 2026
License: MIT

"""
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
    func_type: str = typer.Option("bold", "--func-type", "-f"),
    dry_run: bool = typer.Option(True, "--dry-run/--execute"),
):
    """Rename sbref files to match magnitude files by time using safe two-phase rename."""
    
    func_dir = bids_dir / f"sub-{sub}" / f"ses-{ses}" / "func"
    
    console.print(f"[cyan]Renaming sbref for sub-{sub} ses-{ses}[/cyan]")
    console.print(f"[yellow]Dry run: {dry_run}[/yellow]\n")
    
    # Get magnitude and sbref files
    mag_files = list(func_dir.glob(f"*_{func_type}.nii.gz"))
    sbref_files = list(func_dir.glob("*_sbref.nii.gz"))
    
    console.print(f"Found {len(mag_files)} {func_type}, {len(sbref_files)} sbref\n")
    
    dummy_date = datetime(2000, 1, 1)
    rename_plan = []
    matched_mags = set()  # Track which magnitude files got matched
    unmatched_sbrefs = []  # Track sbrefs that don't match any mag
    
    for sbref_nii in sbref_files:
        sbref_json = sbref_nii.with_suffix('').with_suffix('.json')
        sbref_time = get_acq_time(sbref_json)
        
        if not sbref_time:
            console.print(f"[red]No time for {sbref_nii.name}[/red]")
            unmatched_sbrefs.append(sbref_nii.name)
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
            unmatched_sbrefs.append(sbref_nii.name)
            continue
        
        # Mark this magnitude as matched
        matched_mags.add(best_match)
        
        # Generate new sbref name from magnitude name
        mag_base = best_match.stem.replace('.nii', '').replace(f'_{func_type}', '')
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
    
    # Check for magnitude files without matching sbref
    missing_sbrefs = []
    for mag_nii in mag_files:
        if mag_nii not in matched_mags:
            missing_sbrefs.append(mag_nii.name)
    
    # Display rename plan
    if rename_plan:
        console.print(f"\n[cyan]Rename plan ({len(rename_plan)} files):[/cyan]\n")
        for item in rename_plan:
            console.print(f"[yellow]Match (Δt={item['time_diff']}s):[/yellow]")
            console.print(f"  {item['old_nii'].name}")
            console.print(f"  → {item['new_nii'].name}")
            console.print(f"  (based on {item['mag_file']})\n")
    
    # Report issues
    if missing_sbrefs:
        console.print(f"\n[red]⚠ Missing SBRef files ({len(missing_sbrefs)}):[/red]")
        for mag_name in missing_sbrefs:
            console.print(f"  [red]✗[/red] {mag_name} has no matching sbref")
        console.print()
    
    if unmatched_sbrefs:
        console.print(f"\n[yellow]⚠ Unmatched SBRef files ({len(unmatched_sbrefs)}):[/yellow]")
        for sbref_name in unmatched_sbrefs:
            console.print(f"  [yellow]⚠[/yellow] {sbref_name} has no matching {func_type}")
        console.print()
    
    # Summary
    console.print(f"[bold]Summary:[/bold]")
    console.print(f"  {func_type} files: {len(mag_files)}")
    console.print(f"  SBRef files: {len(sbref_files)}")
    console.print(f"  Matched pairs: {len(matched_mags)}")
    console.print(f"  Need rename: {len(rename_plan)}")
    console.print(f"  Missing SBRef: {len(missing_sbrefs)}")
    console.print(f"  Unmatched SBRef: {len(unmatched_sbrefs)}")
    
    # Execute two-phase rename
    if rename_plan and not dry_run:
        console.print(f"\n[cyan]Phase 1: Rename to temporary names[/cyan]")
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
    elif rename_plan:
        console.print(f"\n[yellow]DRY RUN. Use --execute to rename.[/yellow]")
    else:
        console.print(f"\n[green]No renames needed.[/green]")


if __name__ == "__main__":
    app()