#!/usr/bin/env python3
"""
Check for complete fLoc task runs in BIDS and fMRIPrep directories.

For each subject (01-11) and session (01-10), verifies:
- 10 functional runs with task-fLoc in BIDS (nii.gz)
- 10 functional runs with task-fLoc in fMRIPrep (hemi-L .gii)
- Matching events.tsv files for each run
"""

from pathlib import Path
from typing import Dict
import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer()
console = Console()


def check_bids_runs(bids_dir: Path, subject: str, session: str, expected_runs: int) -> Dict[str, any]:
    """Check BIDS directory for fLoc runs (nii.gz format)."""
    
    func_dir = bids_dir / subject / session / "func"
    
    if not func_dir.exists():
        return {"func_bids": False, "bids_count": 0}
    
    # Find fLoc functional images (nii.gz)
    pattern = f"{subject}_{session}_task-fLoc_run-*_bold.nii.gz"
    nii_files = sorted(func_dir.glob(pattern))
    
    has_all_runs = len(nii_files) == expected_runs
    
    return {
        "func_bids": has_all_runs,
        "bids_count": len(nii_files)
    }


def check_fmriprep_runs(fmriprep_dir: Path, subject: str, session: str, expected_runs: int) -> Dict[str, any]:
    """Check fMRIPrep directory for preprocessed fLoc runs (hemi-L .gii format)."""
    
    func_dir = fmriprep_dir / subject / session / "func"
    
    if not func_dir.exists():
        return {"func_fmriprep": False, "fmriprep_count": 0}
    
    # Find preprocessed fLoc files with hemi-L (left hemisphere gifti)
    pattern = f"{subject}_{session}_task-fLoc_run-*_hemi-L_space-fsnative_bold.func.gii"
    gii_files = sorted(func_dir.glob(pattern))
    
    has_all_runs = len(gii_files) == expected_runs
    
    return {
        "func_fmriprep": has_all_runs,
        "fmriprep_count": len(gii_files)
    }


def check_events(bids_dir: Path, subject: str, session: str, expected_runs: int) -> Dict[str, any]:
    """Check if all events.tsv files exist for each run."""
    
    func_dir = bids_dir / subject / session / "func"
    
    if not func_dir.exists():
        return {"func_events": False, "events_count": 0}
    
    # Find events files
    events_pattern = f"{subject}_{session}_task-fLoc_run-*_events.tsv"
    events_files = sorted(func_dir.glob(events_pattern))
    
    has_all_events = len(events_files) == expected_runs
    
    return {
        "func_events": has_all_events,
        "events_count": len(events_files)
    }


@app.command()
def check_runs(
    bids_dir: Path = typer.Argument(..., help="Path to BIDS directory"),
    fmriprep_dir: Path = typer.Argument(..., help="Path to fMRIPrep output directory"),
    expected_runs: int = typer.Option(10, "--expected", "-e", help="Expected number of runs"),
    output_file: Path = typer.Option("subseslist.txt", "--output", "-o", help="Output file path")
):
    """
    Check if all fLoc runs are present for subjects 01-11 and sessions 01-10.
    
    Creates subseslist.txt with columns: sub, ses, RUN, func_bids, func_fmriprep, func_events
    RUN is True only if all three checks (func_bids, func_fmriprep, func_events) are True.
    """
    
    if not bids_dir.exists():
        console.print(f"[red]Error: BIDS directory not found: {bids_dir}[/red]")
        raise typer.Exit(1)
    
    if not fmriprep_dir.exists():
        console.print(f"[red]Error: fMRIPrep directory not found: {fmriprep_dir}[/red]")
        raise typer.Exit(1)
    
    console.print(f"\n[bold cyan]Checking fLoc runs (expecting {expected_runs} runs per session)[/bold cyan]")
    console.print(f"BIDS directory: {bids_dir}")
    console.print(f"fMRIPrep directory: {fmriprep_dir}")
    console.print(f"Subjects: 01-11, Sessions: 01-10\n")
    
    # Collect all results
    all_results = []
    
    # Fixed subjects and sessions
    subjects = [f"sub-{i:02d}" for i in range(1, 12)]  # sub-01 to sub-11
    sessions = [f"ses-{i:02d}" for i in range(1, 11)]  # ses-01 to ses-10
    
    # Check each subject/session combination
    for subject in subjects:
        for session in sessions:
            # Check BIDS
            bids_result = check_bids_runs(bids_dir, subject, session, expected_runs)
            
            # Check fMRIPrep
            fmriprep_result = check_fmriprep_runs(fmriprep_dir, subject, session, expected_runs)
            
            # Check events
            events_result = check_events(bids_dir, subject, session, expected_runs)
            
            # Combine results
            func_bids = bids_result["func_bids"]
            func_fmriprep = fmriprep_result["func_fmriprep"]
            func_events = events_result["func_events"]
            
            # RUN is True only if all three are True
            run_complete = func_bids and func_fmriprep and func_events
            
            result = {
                "sub": subject,
                "ses": session,
                "RUN": run_complete,
                "func_bids": func_bids,
                "func_fmriprep": func_fmriprep,
                "func_events": func_events,
                "bids_count": bids_result["bids_count"],
                "fmriprep_count": fmriprep_result["fmriprep_count"],
                "events_count": events_result["events_count"]
            }
            
            all_results.append(result)
    
    # Create DataFrame
    df = pd.DataFrame(all_results)
    
    # Save to file (only the main columns)
    df_output = df[["sub", "ses", "RUN", "func_bids", "func_fmriprep", "func_events"]]
    df_output.to_csv(output_file, sep="\t", index=False)
    console.print(f"[green]Results saved to: {output_file}[/green]\n")
    
    # Print summary statistics
    total_sessions = len(df)
    complete_sessions = df["RUN"].sum()
    incomplete_sessions = total_sessions - complete_sessions
    
    console.print(f"[bold]Summary:[/bold]")
    console.print(f"Total sessions: {total_sessions}")
    console.print(f"Complete sessions: {complete_sessions}")
    console.print(f"Incomplete sessions: {incomplete_sessions}\n")
    
    # Filter incomplete sessions
    incomplete_df = df[~df["RUN"]].copy()
    
    if len(incomplete_df) > 0:
        # Print table of incomplete sessions
        table = Table(title="Incomplete Sessions", show_lines=True)
        table.add_column("Subject", style="cyan")
        table.add_column("Session", style="cyan")
        table.add_column("BIDS", justify="center")
        table.add_column("fMRIPrep", justify="center")
        table.add_column("Events", justify="center")
        table.add_column("Details", style="yellow")
        
        for _, row in incomplete_df.iterrows():
            # Build details string
            details = []
            if not row["func_bids"]:
                details.append(f"BIDS: {row['bids_count']}/{expected_runs}")
            if not row["func_fmriprep"]:
                details.append(f"fMRIPrep: {row['fmriprep_count']}/{expected_runs}")
            if not row["func_events"]:
                details.append(f"Events: {row['events_count']}/{expected_runs}")
            
            table.add_row(
                row["sub"],
                row["ses"],
                "✓" if row["func_bids"] else "✗",
                "✓" if row["func_fmriprep"] else "✗",
                "✓" if row["func_events"] else "✗",
                "; ".join(details)
            )
        
        console.print(table)
        
        # Print missing subject/session list
        console.print(f"\n[bold red]Missing/Incomplete subject-session combinations:[/bold red]")
        for _, row in incomplete_df.iterrows():
            console.print(f"  {row['sub']} {row['ses']}")
    else:
        console.print("[bold green]All sessions are complete! ✓[/bold green]")
    
    # Return exit code based on completeness
    if len(incomplete_df) > 0:
        raise typer.Exit(1)


if __name__ == "__main__":
    app()