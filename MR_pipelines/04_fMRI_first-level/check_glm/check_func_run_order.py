#!/usr/bin/env python3
"""
BIDS Functional Run Order Checker with Cross-Validation

This script uses PyBIDS to read functional MRI data from a BIDS dataset,
extracts acquisition times from multiple sources (NIfTI metadata, scans.tsv,
JSON sidecars), cross-validates them, sorts runs by acquisition time, and
validates whether the BIDS run numbering matches the temporal acquisition order.
"""

import os
import re
import json
import warnings
from pathlib import Path
from typing import Optional, List, Dict, Any
from enum import Enum

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich import print as rprint

try:
    from bids import BIDSLayout
except ImportError:
    raise ImportError("PyBIDS not installed. Install with: pip install pybids")

try:
    import nibabel as nib
except ImportError:
    nib = None
    warnings.warn("nibabel not installed. Cannot read NIfTI metadata. Install with: pip install nibabel")

try:
    import pandas as pd
except ImportError:
    pd = None
    warnings.warn("pandas not installed. Cannot read scans.tsv. Install with: pip install pandas")


# Initialize console and app
console = Console()
app = typer.Typer(
    name="check-bids-run-order",
    help="Check if BIDS functional run numbering matches acquisition time order",
    add_completion=False
)


class ValidationStatus(str, Enum):
    """Enum for validation status."""
    VALIDATED = "validated"
    MISMATCH = "mismatch"
    SINGLE_SOURCE = "single_source"
    UNKNOWN = "unknown"


def get_acquisition_time_from_scans_tsv(layout: BIDSLayout, nifti_path: Path) -> Optional[str]:
    """
    Extract acquisition time from scans.tsv file for a specific NIfTI file.
    
    Args:
        layout: BIDSLayout object
        nifti_path: Path to the NIfTI file
        
    Returns:
        Acquisition time string or None
    """
    if pd is None:
        return None
    
    try:
        file_obj = layout.get_file(str(nifti_path))
        if not file_obj:
            return None
        
        entities = file_obj.get_entities()
        subject = entities.get('subject')
        session = entities.get('session')
        
        # Find scans.tsv file
        if session:
            scans_tsv_path = Path(layout.root) / f"sub-{subject}" / f"ses-{session}" / f"sub-{subject}_ses-{session}_scans.tsv"
        else:
            scans_tsv_path = Path(layout.root) / f"sub-{subject}" / f"sub-{subject}_scans.tsv"
        
        if not scans_tsv_path.exists():
            return None, None
        
        df = pd.read_csv(scans_tsv_path, sep='\t')


        if 'filename' not in df.columns:
            return None, None
        # replace all the magnitude to bold
        # Case insensitive
        df['filename'] = df['filename'].str.replace('magnitude', 'bold', case=False)
        df['filename'] = df['filename'].str.replace('T1_uni', 'T1w', case=False)

        nifti_name = Path(nifti_path).name
        matching_rows = df[df['filename'].str.contains(nifti_name, na=False)]
        
        if len(matching_rows) == 0:
            return None, None
        
        row = matching_rows.iloc[0]
        
        # Try different possible column names
        for col in ['acq_time', 'acquisition_time', 'AcquisitionTime']:
            if col in row and pd.notna(row[col]):
                acq_time=str(row[col])
        
       # Extract date from acquisition time
        # Format can be: "2024-12-10T10:30:15" or just "10:30:15"
        acq_date = None
        if acq_time:
            if 'T' in acq_time:
                # Full datetime format: "2024-12-10T10:30:15"
                acq_date = acq_time.split('T')[0]
            elif '-' in acq_time and len(acq_time.split('-')[0]) == 4:
                # Date might be separate or in different format
                # Try to extract YYYY-MM-DD
                import re
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', acq_time)
                if date_match:
                    acq_date = date_match.group(1)
        
        return acq_time, acq_date
        
    except Exception as e:
        console.print(f"[yellow]Warning: Could not read scans.tsv for {nifti_path}: {e}[/yellow]")
        return None, None



def normalize_time_string(time_str: Optional[str]) -> Optional[str]:
    """
    Normalize acquisition time string for comparison.
    
    Args:
        time_str: Time string in various formats
        
    Returns:
        Normalized time string (HH:MM:SS.ffffff format) or None
    """
    if not time_str:
        return None
    
    # Remove timezone info
    time_str = re.sub(r'[+-]\d{2}:?\d{2}$', '', str(time_str))
    time_str = time_str.replace('Z', '').strip()
    
    # If it's a full datetime, extract just the time
    if 'T' in time_str:
        time_str = time_str.split('T')[1]
    
    # If it contains date, extract time
    if ' ' in time_str and ':' in time_str:
        parts = time_str.split()
        for part in parts:
            if ':' in part:
                time_str = part
                break
    
    return time_str


def times_match(time1: Optional[str], time2: Optional[str], tolerance_seconds: float = 1.0) -> bool:
    """
    Check if two time strings match within a tolerance.
    
    Args:
        time1: First time string
        time2: Second time string
        tolerance_seconds: Tolerance in seconds
        
    Returns:
        True if times match within tolerance
    """
    if not time1 or not time2:
        return False
    
    try:
        t1 = normalize_time_string(time1)
        t2 = normalize_time_string(time2)
        
        if not t1 or not t2:
            return False
        
        def parse_time(t: str) -> Optional[float]:
            parts = t.split(':')
            if len(parts) != 3:
                return None
            
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds_parts = parts[2].split('.')
            seconds = int(seconds_parts[0])
            microseconds = int(seconds_parts[1].ljust(6, '0')[:6]) if len(seconds_parts) > 1 else 0
            
            total_seconds = hours * 3600 + minutes * 60 + seconds + microseconds / 1e6
            return total_seconds
        
        t1_sec = parse_time(t1)
        t2_sec = parse_time(t2)
        
        if t1_sec is None or t2_sec is None:
            return t1 == t2
        
        return abs(t1_sec - t2_sec) <= tolerance_seconds
    except:
        return str(time1) == str(time2)

def read_layout(bids_dir: Path) -> BIDSLayout:
    """
    Read BIDS dataset using PyBIDS.
    
    Args:
        bids_dir: Path to BIDS root directory
        
    Returns:
        BIDSLayout object
    """
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        progress.add_task(description="Indexing BIDS dataset...", total=None)
        layout = BIDSLayout(str(bids_dir), validate=False, derivatives=False)
    
    console.print(f"[green]✓[/green] Dataset indexed successfully")

    return layout
    
def find_functional_runs(layout: BIDSLayout, task: str) -> List[Dict[str, Any]]:
    """
    Find all functional runs for a specific task in a BIDS directory using PyBIDS.
    
    Args:
        bids_dir: Path to BIDS root directory
        task: Task name to filter
        
    Returns:
        List of dictionaries containing run information
    """
    
    # Get all functional runs for the specified task
    bold_files = layout.get(
        suffix='bold',
        task=task,
        extension='.nii.gz',
        return_type='file'
    )
    
    if not bold_files:
        console.print(f"[yellow]Warning: No functional runs found for task '{task}'[/yellow]")
        return []
    
    console.print(f"[cyan]Found {len(bold_files)} functional run(s) for task '{task}'[/cyan]")
    
    runs = []
    
    with Progress(console=console) as progress:
        task_progress = progress.add_task("[cyan]Processing runs...", total=len(bold_files))
        
        for nifti_file in bold_files:
            nifti_path = Path(nifti_file)
            
            # Get entities from PyBIDS
            file_obj = layout.get_file(str(nifti_file))
            entities = file_obj.get_entities()
            
            sub = entities['subject']
            ses = entities['session']
            run = entities['run']
            # Get corresponding JSON metadata
            json_metadata = layout.get_metadata(str(nifti_file))
            
            # Get acquisition times from different sources
            acq_time_json = json_metadata.get('AcquisitionTime') or json_metadata.get('AcquisitionDateTime')
            acq_time_tsv, acq_date_tsv = get_acquisition_time_from_scans_tsv(layout, nifti_file)

            
            # Cross-validate acquisition times
            acq_time_sources = {}
            
            if acq_time_json:
                acq_time_sources['json'] = acq_time_json
            if acq_time_tsv:
                acq_time_sources['scans_tsv'] = acq_time_tsv
            
            if not acq_time_tsv:
                console.print(f"[yellow]Warning: No acquisition time in the scans.tsv found for {nifti_path.name}[/yellow]")
            if not acq_time_json:
                console.print(f"[yellow]Warning: No acquisition time in the json found for {nifti_path.name}[/yellow]")          
            # Check if all sources agree
            all_times = [t for t in [acq_time_json, acq_time_tsv] if t]
            if len(all_times) > 1:
                matches = all(times_match(all_times[0], t) for t in all_times[1:])
                validation_status = ValidationStatus.VALIDATED if matches else ValidationStatus.MISMATCH
            elif len(all_times) == 1:
                validation_status = ValidationStatus.SINGLE_SOURCE
            else:
                validation_status = ValidationStatus.UNKNOWN
            
            # Store run information
            runs.append({
                'file': nifti_path,
                'filename': nifti_path.name,
                'sub': sub,
                'ses': ses,
                'bids_run': run,
                'acq_time_tsv': normalize_time_string(acq_time_tsv),
                'acq_time_json': acq_time_json,
                'acq_date_tsv': acq_date_tsv,
                'acq_time_sources': acq_time_sources,
                'validation_status': validation_status
            })
            
            progress.advance(task_progress)

    return runs

def display_mismatch_details(sorted_runs: List[Dict[str, Any]], group_key: str) -> List[Dict]:
    """Display detailed mismatch information and return mismatch list."""
    
    table = Table(title=f"Mismatched Runs: {group_key}", show_header=True, header_style="bold red")
    table.add_column("Current BIDS", justify="center", style="red")
    table.add_column("Should Be", justify="center", style="green")
    table.add_column("Acq Time", style="dim")
    table.add_column("Filename", style="dim")
    
    mismatches = []
    
    for temporal_idx, run in enumerate(sorted_runs, start=1):
        bids_run = run['entities'].get('run', None)
        
        if bids_run != temporal_idx:
            mismatches.append({
                'current_run': bids_run,
                'should_be_run': temporal_idx,
                'acq_time': run['acq_time_json'],
                'filename': run['filename']
            })
            
            table.add_row(
                f"run-{bids_run:02d}",
                f"run-{temporal_idx:02d}",
                str(run['acq_time_json']),
                run['filename']
            )
    
    if mismatches:
        console.print(table)
    
    return mismatches


def check_run_order(
    bids_dir: Path,
    layout: BIDSLayout,
    task: str,
    verbose: bool = True
) -> Optional[Dict[str, Any]]:
    """
    Check if BIDS run numbering matches acquisition time order.
    
    Args:
        bids_dir: Path to BIDS root directory
        task: Task name to filter
        verbose: Print detailed information
        
    Returns:
        Dictionary with results or None
    """
    # Find all runs using PyBIDS
    runs = find_functional_runs(layout, task)
    
    if not runs:
        console.print(f"[red]No functional runs found for task '{task}' in {bids_dir}[/red]")
        return None
    
    # Convert runs to DataFrame
    df = pd.DataFrame(runs)
    results = {}
    
    for (subject, session), group_df in df.groupby(['sub', 'ses']):
        # Sort by acquisition time
        group_df = group_df.sort_values('acq_time_json').reset_index(drop=True)
        date = group_df['acq_date_tsv'].iloc[0]
        
        # Add temporal run index (what run number should be based on time)
        group_df['temporal_run'] = range(1, len(group_df) + 1)
        
        # Check if BIDS run matches temporal order
        group_df['is_mismatch'] = group_df['bids_run'] != group_df['temporal_run']
        
        # Get mismatches
        mismatches_df = group_df[group_df['is_mismatch']]
        
        mismatches = mismatches_df[['bids_run', 'temporal_run', 'acq_time_json', 'filename']].to_dict('records')
        
        # Create session key for results
        session_key = f"sub-{subject}_ses-{session}" if session != 'none' else f"sub-{subject}"
        
        results[session_key] = {
            'total_runs': len(group_df),
            'mismatches': mismatches,
            'all_correct': len(mismatches) == 0,
            'sorted_runs': group_df.to_dict('records')
        }
    

    return results

def create_results_dataframe(results: Dict[str, Any]) -> pd.DataFrame:
    """
    Create a DataFrame report from the results.
    
    Args:
        results: Dictionary with check results
        
    Returns:
        DataFrame with columns: subject, session, bids_run, actual_run, is_correct, acq_date
    """
    report_data = []
    
    for session_key, session_data in results.items():
        for run in session_data['sorted_runs']:
            # Parse session_key to get subject and session
            if '_ses-' in session_key:
                subject = session_key.split('_ses-')[0].replace('sub-', '')
                session = session_key.split('_ses-')[1]
            else:
                subject = session_key.replace('sub-', '')
                session = 'none'
            
            bids_run = run['bids_run']
            actual_run = run['temporal_run']
            acq_date = run['acq_date_tsv']
            
            report_data.append({
                'subject': subject,
                'session': session,
                'acq_date': acq_date,  # Add date column
                'bids_run': bids_run,
                'actual_run': actual_run,
                'is_correct': bids_run == actual_run,
                'filename': run['filename'],
                'acq_time': run['acq_time_json']
            })
    
    df_report = pd.DataFrame(report_data)
    return df_report

def print_results(results: Dict[str, Any], quiet: bool = False) -> None:
    """
    Print the results of run order checking.
    
    Args:
        results: Dictionary with check results
        quiet: If True, suppress detailed output
    """
    if not results:
        console.print("[red]No results to display[/red]")
        return
    
    # Filter for mismatched sessions
    mismatched_sessions = {k: v for k, v in results.items() if not v['all_correct']}
    
    if mismatched_sessions:
        console.print(f"[bold red]{'='*80}[/bold red]")
        console.print(Panel.fit(
            f"[bold red]Found {len(mismatched_sessions)} session(s) with MISMATCHED run ordering[/bold red]",
            border_style="red"
        ))
        console.print(f"[bold red]{'='*80}[/bold red]\n")
        
        if not quiet:
            # Show details for each mismatched session
            for session_key, session_data in mismatched_sessions.items():
                console.print(f"\n[bold yellow]{'─'*80}[/bold yellow]")
                console.print(f"[bold yellow]SESSION: {session_key}[/bold yellow]")
                console.print(f"[bold yellow]{'─'*80}[/bold yellow]")
                console.print(f"Total runs: {session_data['total_runs']}")
                console.print(f"Mismatched runs: [red]{len(session_data['mismatches'])}[/red]\n")
                
                # Show why each run is wrong
                console.print("[bold red]REASON - Run order does not match acquisition time:[/bold red]\n")
                
                for mismatch in session_data['mismatches']:
                    console.print(f"  [red]✗[/red] File: [cyan]{mismatch['filename']}[/cyan]")
                    console.print(f"    Current BIDS name: [red]run-{mismatch['bids_run']:02d}[/red]")
                    console.print(f"    Should be named:   [green]run-{mismatch['temporal_run']:02d}[/green]")
                    console.print(f"    Acquisition time:  {mismatch['acq_time_json']}")
                    console.print()
    else:
        console.print(Panel(
            "[bold green]✓ All sessions have correctly numbered runs![/bold green]\n"
            "All run numbers match their acquisition time order.",
            style="green"
        ))
    
    # Overall summary
    console.print(f"\n[bold cyan]{'='*80}[/bold cyan]")
    
    total_sessions = len(results)
    correct_sessions = total_sessions - len(mismatched_sessions)
    
    summary_table = Table(title="Overall Summary", show_header=True, header_style="bold magenta")
    summary_table.add_column("Metric", style="cyan")
    summary_table.add_column("Value", justify="right")
    
    summary_table.add_row("Total sessions checked", str(total_sessions))
    summary_table.add_row("Sessions with correct ordering", f"[green]{correct_sessions}[/green]")
    summary_table.add_row("Sessions with MISMATCHED ordering", f"[red]{len(mismatched_sessions)}[/red]")
    
    console.print(summary_table)
    
    # List mismatched sessions
    if mismatched_sessions:
        console.print("\n[bold red]Sessions requiring correction:[/bold red]")
        for session_key in mismatched_sessions.keys():
            console.print(f"  • {session_key}")
        console.print()

def get_session_dates(report_df: pd.DataFrame) -> pd.DataFrame:
    """
    Get unique acquisition date for each session.
    
    Args:
        report_df: DataFrame with subject, session, acq_date columns
        
    Returns:
        DataFrame with subject, session, acq_date (one row per session)
    """
    # Group by subject and session, get the first date (should be same for all runs in session)
    session_dates = report_df.groupby(['subject', 'session'])['acq_date'].first().reset_index()
    return session_dates

def convert_date_to_folder_format(date_str: str) -> str:
    """
    Convert date from "2025-02-26" to "26-Feb-2025".
    
    Args:
        date_str: Date in format "YYYY-MM-DD"
        
    Returns:
        Date in format "DD-MMM-YYYY"
    """
    from datetime import datetime
    if date_str:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    else:
        return None
    return date_obj.strftime("%d-%b-%Y")

def verify_sourcedata_dates(
    floc_onset_dir: Path,
    acq_date: str
) -> None:
    """
    Verify that sourcedata directory names contain the correct acquisition dates.
    
    Args:
        floc_onset_dir: Path to sourcedata directory
        acq_date: acq_date for the session in "YYYY-MM-DD" format
    """
    console.print("[cyan]  Verifying sourcedata dates...[cyan]\n")
    # Check if date is in directory name
    # Expected format might be: sub-XX_ses-YY_1back_YYYY-MM-DD or similar
    dir_name = floc_onset_dir.name
    if acq_date and acq_date in dir_name:
        console.print(f"[green]✓: Date {acq_date} matches directory name[/green]")
        matching = True
    elif acq_date:
        console.print(f"[yellow]⚠: Date {acq_date} NOT found in onset_dir name: {dir_name}[/yellow]")
        matching = False
    else:
        console.print(f"[yellow]⚠: No date scans.tsv and no date info for verification[/yellow]")
        console.print(f"  Directory: {dir_name}")
        matching = False

    return matching
    
def create_events_symlinks(
    bids_dir: Path,
    layout: BIDSLayout,
    sourcedata_dir: Path,
    report_df: pd.DataFrame,
    task: str,
    force: bool = False,
    dry_run: bool = True
) -> None:
    """
    Create symbolic links for events.tsv files from sourcedata to BIDS func directory.
    
    For correct runs: links to the same run number in sourcedata
    For mismatched runs: links to the actual temporal run number in sourcedata
    
    Args:
        bids_dir: Path to BIDS root directory
        sourcedata_dir: Path to sourcedata directory containing onset files
        report_df: DataFrame with subject, session, bids_run, actual_run, is_correct
        task: Task name
        force: If True, overwrite existing symlinks
        dry_run: If True, only show what would be done
    """
    console.print("\n[bold cyan]Creating events.tsv symlinks...[/bold cyan]\n")
        
    symlink_count = 0
    ses_error_count = 0
    
    # Group by subject and session
    for (subject, session), group_df in report_df.groupby(['subject', 'session']):

        # Get onset directory name
        if session != 'none':
            onset_subses_dir = sourcedata_dir / f'sub-{subject}' / f'ses-{session}'
        else:
            onset_subses_dir = sourcedata_dir / f'sub-{subject}'
        
        # Find the onset directory (contains '1back' in name)
        if not onset_subses_dir.exists():
            console.print(f"[red]✗ Source directory not found: {onset_subses_dir}[/red]")
            ses_error_count += 1
            continue
        
        votcloc_logs = [d for d in onset_subses_dir.iterdir() 
                       if d.is_dir() and '1back' in d.name]
        
        onset_dirname = None
        subses = f'sub-{subject}_ses-{session}' if session != 'none' else f'sub-{subject}'
        
        for log_dir in votcloc_logs:
            if subses in log_dir.name:
                onset_dirname = log_dir.name
                break
        
        if not onset_dirname:
            console.print(f"[yellow]⚠ No onset directory found for {subses}[/yellow]")
            ses_error_count += 1
            continue
        
        all_onset_dir = onset_subses_dir / onset_dirname
        
        console.print(f"\n[cyan]Processing {subses}...[/cyan]")
        console.print(f"  Onset directory: {onset_dirname}")
        acq_date = group_df['acq_date'].unique()[0]
        acq_date = convert_date_to_folder_format(acq_date)
        console.print(f"  acq_date from scans.tsv directory: {acq_date}")
        date_match = verify_sourcedata_dates(all_onset_dir, acq_date)
        if date_match:
            # Process each run
            run_error_count = 0
            for _, row in group_df.iterrows():
                bids_run = row['bids_run']
                actual_run = row['actual_run']
                is_correct = row['is_correct']
                
                # Build paths
                if session != 'none':
                    func_dir = bids_dir / f"sub-{subject}" / f"ses-{session}" / "func"
                    bids_events = func_dir / f"sub-{subject}_ses-{session}_task-{task}_run-{bids_run:02d}_events.tsv"
                else:
                    func_dir = bids_dir / f"sub-{subject}" / "func"
                    bids_events = func_dir / f"sub-{subject}_task-{task}_run-{bids_run:02d}_events.tsv"
                
                # Determine source events file
                # If correct: link to same run number
                # If incorrect: link to actual temporal run number (sourcedata follows timing order)
                if is_correct:
                    source_run = bids_run
                    link_type = "[green]correct[/green]"
                else:
                    source_run = actual_run
                    link_type = "[yellow]corrected[/yellow]"
                
                if session != 'none':
                    src_events = all_onset_dir / f"sub-{subject}_ses-{session}_task-{task}_run-{source_run:02d}_events.tsv"
                else:
                    src_events = all_onset_dir / f"sub-{subject}_task-{task}_run-{source_run:02d}_events.tsv"
                
                # Check if source exists
                if not src_events.exists():
                    console.print(f"  [red]✗ Source not found: {src_events.name}[/red]")
                    run_error_count += 1
                    continue
                
                # Check if target already exists
                if bids_events.exists() or bids_events.is_symlink():
                    if not force:
                        if dry_run:
                            console.print(f"  [dim]- Target exists, would skip (use --force to overwrite): {bids_events.name}[/dim]")
                        else:
                            console.print(f"  [dim]- Target exists, skipping: {bids_events.name}[/dim]")
                        continue
                    else:
                        if dry_run:
                            console.print(f"  [yellow]Would unlink existing: {bids_events.name}[/yellow]")
                        else:
                            bids_events.unlink()
                            console.print(f"  [yellow]Unlinked existing: {bids_events.name}[/yellow]")
                
                # Create func directory if it doesn't exist
                if not dry_run and not func_dir.exists():
                    func_dir.mkdir(parents=True, exist_ok=True)
                
                if dry_run:
                    console.print(f"  [cyan]Would create symlink ({link_type}):[/cyan]")
                    console.print(f"    Target: {bids_events.name}")
                    console.print(f"    Source: {src_events.name}")
                else:
                    try:
                        bids_events.symlink_to(src_events)
                        console.print(f"  [green]✓ Created symlink ({link_type}):[/green]")
                        console.print(f"    Target: {bids_events.name}")
                        console.print(f"    Source: {src_events.name}")
                        symlink_count += 1
                    except Exception as e:
                        console.print(f"  [red]✗ Error creating symlink: {e}[/red]")
                        run_error_count += 1
            if run_error_count > 0:
                ses_error_count += 1
        else:
            console.print(f"[red]✗ Skipping symlink creation for {subses} due to date mismatch[/red]")
            ses_error_count += 1

    # Summary
    console.print(f"\n[bold cyan]{'='*80}[/bold cyan]")
    if dry_run:
        console.print(f"[bold yellow]DRY RUN: Would create {symlink_count} symlinks[/bold yellow]")
        if ses_error_count > 0:
            console.print(f"[bold red]Encountered {ses_error_count} errors[/bold red]")
        console.print("[dim]Run with --apply to actually create the symlinks[/dim]")
    else:
        console.print(f"[bold green]✓ Successfully created {symlink_count} symlinks[/bold green]")
        if ses_error_count > 0:
            console.print(f"[bold red]Encountered {ses_error_count} errors[/bold red]")


def verify_sourcedata_events(
    sourcedata_dir: Path,
    report_df: pd.DataFrame,
    expected_runs: int = 10
) -> None:
    """
    Verify that sourcedata has the expected number of events.tsv files.
    
    Args:
        sourcedata_dir: Path to sourcedata directory
        report_df: DataFrame with subject and session info
        expected_runs: Expected number of runs per session
    """
    console.print("\n[bold cyan]Verifying sourcedata events.tsv files...[/bold cyan]\n")
    
    issues_found = False
    
    for (subject, session), group_df in report_df.groupby(['subject', 'session']):
        if session != 'none':
            onset_subses_dir = sourcedata_dir / f'sub-{subject}' / f'ses-{session}'
        else:
            onset_subses_dir = sourcedata_dir / f'sub-{subject}'
        
        if not onset_subses_dir.exists():
            console.print(f"[red]✗ Source directory not found: {onset_subses_dir}[/red]")
            issues_found = True
            continue
        
        # Find onset directory
        votcloc_logs = [d for d in onset_subses_dir.iterdir() 
                       if d.is_dir() and '1back' in d.name]
        
        subses = f'sub-{subject}_ses-{session}' if session != 'none' else f'sub-{subject}'
        onset_dirname = None
        
        for log_dir in votcloc_logs:
            if subses in log_dir.name:
                onset_dirname = log_dir.name
                break
        
        if not onset_dirname:
            console.print(f"[yellow]⚠ No onset directory found for {subses}[/yellow]")
            issues_found = True
            continue
        
        all_onset_dir = onset_subses_dir / onset_dirname
        
        # Find all events.tsv files
        events_files = list(all_onset_dir.rglob("*events.tsv"))
        
        if len(events_files) != expected_runs:
            console.print(f"[yellow]⚠ {subses}: Found {len(events_files)} events.tsv files (expected {expected_runs})[/yellow]")
            issues_found = True
        else:
            console.print(f"[green]✓ {subses}: Found {len(events_files)} events.tsv files[/green]")
    
    if not issues_found:
        console.print("\n[bold green]✓ All sessions have the expected number of events.tsv files[/bold green]")


@app.command()
def main(
    bids_dir: Path = typer.Argument(
        ...,
        help="Path to BIDS dataset root directory",
        exists=True,
        file_okay=False,
        dir_okay=True,
        resolve_path=True
    ),
    sourcedata_dir: Path = typer.Option(
        ...,
        "--sourcedata",
        help="Path to sourcedata directory containing onset files",
        exists=True,
        file_okay=False,
        dir_okay=True,
        resolve_path=True
    ),
    task: str = typer.Option(
        "fLoc",
        "--task", "-t",
        help="Task name to check"
    ),
    output_csv: Optional[Path] = typer.Option(
        None,
        "--output", "-o",
        help="Save report to CSV file"
    ),
    create_symlinks: bool = typer.Option(
        False,
        "--create-symlinks", "-cs",
        help="Create events.tsv symlinks (dry run by default)"
    ),
    verify_source: bool = typer.Option(
        False,
        "--verify-source",
        help="Verify sourcedata has expected events.tsv files"
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Force overwrite existing symlinks"
    ),
    run: bool = typer.Option(
        False,
        "--run",
        help="Actually apply changes (use with --create-symlinks)"
    ),
    quiet: bool = typer.Option(
        False,
        "--quiet", "-q",
        help="Suppress detailed output, only show summary"
    )
):
    """
    Check if BIDS functional run numbering matches acquisition time order.
    
    Features:
    - Check run ordering against acquisition times
    - Generate CSV report
    - Create events.tsv symlinks with correct temporal mapping
    - Verify sourcedata events.tsv files
    """
    
    # Display header
    console.print("\n")
    console.print(Panel.fit(
        "[bold cyan]BIDS Functional Run Order Checker[/bold cyan]\n"
        "[dim]with Events Symlink Creation[/dim]",
        border_style="cyan"
    ))
    console.print("\n")
    # Read BIDS dataset
    layout = read_layout(bids_dir)

    # Check run order
    results = check_run_order(bids_dir, layout, task=task, verbose=not quiet)
    if not results:
        raise typer.Exit(code=1)
    
    # Print results
    print_results(results, quiet=quiet)
    
    # Create report DataFrame
    report_df = create_results_dataframe(results)
    # Save to CSV if requested
    if output_csv:
        report_df.to_csv(output_csv, index=False)
        console.print(f"\n[green]✓ Report saved to: {output_csv}[/green]")    
    # Get session dates
    # session_dates_df = get_session_dates(report_df)    
    
    # Verify sourcedata events.tsv files if requested
    if verify_source:
        verify_sourcedata_events(sourcedata_dir, report_df)
    
    # Create events.tsv symlinks if requested
    if create_symlinks:
        create_events_symlinks(
            bids_dir, 
            layout,
            sourcedata_dir, 
            report_df, 
            task, 
            force=force,
            dry_run=not run
        )
    
    # Exit with appropriate code
    mismatched_sessions = {k: v for k, v in results.items() if not v['all_correct']}
    exit_code = 1 if mismatched_sessions else 0
    raise typer.Exit(code=exit_code)


if __name__ == '__main__':
    app()