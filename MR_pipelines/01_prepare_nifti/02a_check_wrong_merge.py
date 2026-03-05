#!/usr/bin/env python3
"""
Check all scans.tsv files in a BIDS dataset for:
1. Multiple session IDs in one scans.tsv file
2. Non-ascending run numbers based on acquisition time
"""
import typer
from pathlib import Path
import pandas as pd
from collections import defaultdict
from rich.console import Console
from rich.table import Table
from typing import Optional
import re

app = typer.Typer()
console = Console()


def parse_bids_filename(filename: str):
    """Parse BIDS filename to extract entities."""
    parts = filename.split('_')
    entities = {}
    
    for part in parts:
        if '-' in part:
            key, val = part.split('-', 1)
            entities[key] = val
    
    # Get suffix (modality label, last part without extension)
    stem = filename.replace('.nii.gz', '').replace('.json', '').replace('.tsv', '').replace('.bval', '').replace('.bvec', '')
    suffix = stem.split('_')[-1]
    entities['suffix'] = suffix
    
    return entities


def normalize_run_key(filename: str, entities: dict, modality: str) -> Optional[str]:
    """
    Create a normalized key for grouping runs that should be checked together.
    
    Special handling:
    - T1 MP2RAGE: inv1, inv2, uni are the same acquisition, use same run number
    - DWI: AP and PA are part of the same acquisition sequence
    - fmap: AP and PA are part of the same acquisition sequence
    
    Returns:
        A key string for grouping, or None if this file should be ignored for run checks
    """
    run = entities.get('run')
    if not run:
        return None
    
    task = entities.get('task')
    suffix = entities.get('suffix')
    acq = entities.get('acq')
    direction = entities.get('dir')
    
    # T1 MP2RAGE: inv1, inv2, uni are all the same run
    if modality == 'anat' and suffix in ['inv1', 'inv2', 'uni', 'T1']:
        # Normalize by removing the direction, use only run number
        return f"anat_T1w_run-{run}"
    
    # DWI: AP and PA are part of same acquisition, normalize without direction
    if modality == 'dwi':
        # Group by run number only, ignore direction (AP/PA)
        if acq:
            return f"dwi_acq-{acq}_run-{run}"
        else:
            return f"dwi_run-{run}"
    
    # fmap: AP and PA are part of same acquisition, normalize without direction
    if modality == 'fmap':
        # Group by run and acq, ignore direction (AP/PA)
        if acq:
            return f"fmap_acq-{acq}_run-{run}"
        else:
            return f"fmap_run-{run}"
    
    # Functional: group by task
    if modality == 'func':
        if task:
            # For func, magnitude, phase, and sbref are all same run
            return f"func_task-{task}_run-{run}"
        else:
            return f"func_run-{run}"
    
    # Default: use modality and run
    return f"{modality}_run-{run}"


def check_scans_tsv(scans_file: Path, expected_sub: str, expected_ses: str):
    """
    Check a single scans.tsv file for issues.
    
    Returns:
        dict with 'session_issues' and 'run_order_issues'
    """
    issues = {
        'session_issues': [],
        'run_order_issues': [],
        'file': scans_file
    }
    
    try:
        df = pd.read_csv(scans_file, sep='\t')
    except Exception as e:
        issues['session_issues'].append(f"Could not read file: {e}")
        return issues
    
    if len(df) == 0:
        return issues
    
    # Check 1: Multiple session IDs
    sessions_found = set()
    for idx, row in df.iterrows():
        filename = row['filename']
        entities = parse_bids_filename(filename)
        
        ses = entities.get('ses')
        if ses:
            sessions_found.add(ses)
    
    if len(sessions_found) > 1:
        issues['session_issues'].append(
            f"Multiple sessions found: {', '.join(sorted(sessions_found))} (expected: {expected_ses})"
        )
    elif len(sessions_found) == 1:
        found_ses = list(sessions_found)[0]
        if found_ses != expected_ses:
            issues['session_issues'].append(
                f"Session mismatch: found ses-{found_ses}, expected ses-{expected_ses}"
            )
    
    # Check 2: Run number ordering per normalized group
    # Sort by acquisition time
    df_sorted = df.sort_values('acq_time').copy()
    
    # Track unique runs per normalized group
    run_groups = defaultdict(lambda: defaultdict(list))
    
    for idx, row in df_sorted.iterrows():
        filename = row['filename']
        acq_time = row['acq_time']
        
        entities = parse_bids_filename(filename)
        run = entities.get('run')
        
        # Get modality from filename path
        if '/' in filename:
            modality = filename.split('/')[0]
        else:
            modality = 'unknown'
        
        if run:
            # Get normalized key
            norm_key = normalize_run_key(filename, entities, modality)
            
            if norm_key:
                run_num = int(run)
                
                # Store first occurrence of this run in this group
                if run_num not in run_groups[norm_key]:
                    run_groups[norm_key][run_num] = {
                        'run': run_num,
                        'acq_time': acq_time,
                        'filename': filename,
                        'first_occurrence': True
                    }
    
    # Check if runs are ascending within each group
    for norm_key, runs_dict in run_groups.items():
        if len(runs_dict) < 2:
            continue
        
        # Get runs in order of first appearance (by time)
        runs_list = list(runs_dict.values())
        run_numbers = [r['run'] for r in runs_list]
        
        # Check for non-ascending (runs should appear in order 1, 2, 3, ...)
        for i in range(1, len(run_numbers)):
            if run_numbers[i] <= run_numbers[i-1]:
                issues['run_order_issues'].append({
                    'group': norm_key,
                    'previous': runs_list[i-1],
                    'current': runs_list[i],
                    'problem': f"Run {run_numbers[i]} comes after run {run_numbers[i-1]} in chronological order"
                })
        
        # Check for proper sequence (1, 2, 3, ... with no gaps)
        sorted_runs = sorted(run_numbers)
        expected_runs = list(range(1, len(run_numbers) + 1))
        
        if sorted_runs != expected_runs:
            # Check if starts at 1
            if sorted_runs[0] != 1:
                issues['run_order_issues'].append({
                    'group': norm_key,
                    'problem': f"Runs don't start at 1 (starts at {sorted_runs[0]})",
                    'runs': sorted_runs
                })
            
            # Check for gaps
            for i in range(len(sorted_runs) - 1):
                if sorted_runs[i+1] - sorted_runs[i] > 1:
                    issues['run_order_issues'].append({
                        'group': norm_key,
                        'problem': f"Gap in run numbers: {sorted_runs[i]} → {sorted_runs[i+1]}",
                        'runs': sorted_runs
                    })
    
    return issues


def find_all_scans_tsv(bids_dir: Path):
    """Find all scans.tsv files in the BIDS directory."""
    scans_files = []
    
    for sub_dir in sorted(bids_dir.glob("sub-*")):
        if not sub_dir.is_dir():
            continue
        
        subject = sub_dir.name.replace('sub-', '')
        
        for ses_dir in sorted(sub_dir.glob("ses-*")):
            if not ses_dir.is_dir():
                continue
            
            session = ses_dir.name.replace('ses-', '')
            
            # Look for scans.tsv
            scans_file = ses_dir / f"sub-{subject}_ses-{session}_scans.tsv"
            if scans_file.exists():
                scans_files.append({
                    'file': scans_file,
                    'subject': subject,
                    'session': session
                })
    
    return scans_files


@app.command()
def check_all(
    bids_dir: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=False,
        dir_okay=True,
        help="Path to BIDS directory"
    ),
    output_file: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Save detailed report to file"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show all checks including files with no issues"
    )
):
    """
    Check all scans.tsv files in a BIDS dataset for:
    1. Multiple session IDs in one file
    2. Non-ascending run numbers based on acquisition time
    
    Smart handling of special cases:
    - T1 MP2RAGE: inv1, inv2, uni treated as same run
    - DWI: AP and PA directions treated as same run
    - fmap: AP and PA directions treated as same run
    
    Examples:
        check_scans_tsv.py /path/to/bids
        check_scans_tsv.py /path/to/bids --output report.txt
        check_scans_tsv.py /path/to/bids --verbose
    """
    console.print(f"[bold]Checking scans.tsv files in BIDS dataset[/bold]")
    console.print(f"  BIDS directory: {bids_dir}\n")
    
    # Find all scans.tsv files
    scans_files = find_all_scans_tsv(bids_dir)
    
    if not scans_files:
        console.print("[yellow]No scans.tsv files found[/yellow]")
        return
    
    console.print(f"Found {len(scans_files)} scans.tsv files\n")
    
    # Check each file
    all_results = []
    files_with_issues = 0
    
    with console.status("[cyan]Checking files...") as status:
        for item in scans_files:
            scans_file = item['file']
            subject = item['subject']
            session = item['session']
            
            status.update(f"[cyan]Checking sub-{subject}/ses-{session}...")
            
            issues = check_scans_tsv(scans_file, subject, session)
            
            has_issues = (len(issues['session_issues']) > 0 or 
                         len(issues['run_order_issues']) > 0)
            
            if has_issues:
                files_with_issues += 1
            
            all_results.append({
                'subject': subject,
                'session': session,
                'issues': issues,
                'has_issues': has_issues
            })
    
    # Print summary
    console.print(f"\n[bold]Summary:[/bold]")
    console.print(f"  Total files checked: {len(scans_files)}")
    console.print(f"  [green]✓ Files with no issues: {len(scans_files) - files_with_issues}[/green]")
    console.print(f"  [red]✗ Files with issues: {files_with_issues}[/red]")
    
    # Print detailed results
    if files_with_issues > 0:
        console.print(f"\n[bold red]Files with Issues:[/bold red]\n")
        
        for result in all_results:
            if not result['has_issues']:
                continue
            
            subject = result['subject']
            session = result['session']
            issues = result['issues']
            
            console.print(f"[cyan]sub-{subject}/ses-{session}[/cyan]")
            console.print(f"  File: {issues['file']}")
            
            # Session issues
            if issues['session_issues']:
                console.print(f"  [yellow]Session ID issues:[/yellow]")
                for issue in issues['session_issues']:
                    console.print(f"    - {issue}")
            
            # Run order issues
            if issues['run_order_issues']:
                console.print(f"  [yellow]Run order issues:[/yellow]")
                for issue in issues['run_order_issues']:
                    if 'previous' in issue:
                        console.print(f"    - {issue['group']}: {issue['problem']}")
                        console.print(f"      Previous: run-{issue['previous']['run']:02d} @ {issue['previous']['acq_time']}")
                        console.print(f"                {issue['previous']['filename']}")
                        console.print(f"      Current:  run-{issue['current']['run']:02d} @ {issue['current']['acq_time']}")
                        console.print(f"                {issue['current']['filename']}")
                    else:
                        console.print(f"    - {issue['group']}: {issue['problem']}")
                        if 'runs' in issue:
                            console.print(f"      Runs found: {issue['runs']}")
            
            console.print()
    
    # Show clean files if verbose
    if verbose and (len(scans_files) - files_with_issues) > 0:
        console.print(f"\n[bold green]Files with no issues:[/bold green]\n")
        for result in all_results:
            if result['has_issues']:
                continue
            console.print(f"  [green]✓[/green] sub-{result['subject']}/ses-{result['session']}")
    
    # Export detailed report if requested
    if output_file:
        export_report(all_results, output_file)
        console.print(f"\n[cyan]Detailed report saved to: {output_file}[/cyan]")


def export_report(results, output_file: Path):
    """Export detailed report to text file."""
    with open(output_file, 'w') as f:
        f.write("SCANS.TSV CHECK REPORT\n")
        f.write("=" * 80 + "\n\n")
        
        f.write("Smart checking rules applied:\n")
        f.write("  - T1 MP2RAGE: inv1, inv2, uni treated as same run\n")
        f.write("  - DWI: AP and PA directions treated as same run\n")
        f.write("  - fmap: AP and PA directions treated as same run\n\n")
        
        files_with_issues = sum(1 for r in results if r['has_issues'])
        
        f.write(f"Total files checked: {len(results)}\n")
        f.write(f"Files with no issues: {len(results) - files_with_issues}\n")
        f.write(f"Files with issues: {files_with_issues}\n\n")
        
        f.write("=" * 80 + "\n\n")
        
        # Files with issues
        if files_with_issues > 0:
            f.write("FILES WITH ISSUES:\n\n")
            
            for result in results:
                if not result['has_issues']:
                    continue
                
                subject = result['subject']
                session = result['session']
                issues = result['issues']
                
                f.write(f"sub-{subject}/ses-{session}\n")
                f.write(f"  File: {issues['file']}\n")
                
                if issues['session_issues']:
                    f.write(f"  Session ID issues:\n")
                    for issue in issues['session_issues']:
                        f.write(f"    - {issue}\n")
                
                if issues['run_order_issues']:
                    f.write(f"  Run order issues:\n")
                    for issue in issues['run_order_issues']:
                        if 'previous' in issue:
                            f.write(f"    - {issue['group']}: {issue['problem']}\n")
                            f.write(f"      Previous: run-{issue['previous']['run']:02d} @ {issue['previous']['acq_time']}\n")
                            f.write(f"        {issue['previous']['filename']}\n")
                            f.write(f"      Current:  run-{issue['current']['run']:02d} @ {issue['current']['acq_time']}\n")
                            f.write(f"        {issue['current']['filename']}\n")
                        else:
                            f.write(f"    - {issue['group']}: {issue['problem']}\n")
                            if 'runs' in issue:
                                f.write(f"      Runs found: {issue['runs']}\n")
                
                f.write("\n")
        
        # Clean files
        f.write("\n" + "=" * 80 + "\n\n")
        f.write("FILES WITH NO ISSUES:\n\n")
        
        for result in results:
            if result['has_issues']:
                continue
            f.write(f"  ✓ sub-{result['subject']}/ses-{result['session']}\n")


if __name__ == "__main__":
    app()