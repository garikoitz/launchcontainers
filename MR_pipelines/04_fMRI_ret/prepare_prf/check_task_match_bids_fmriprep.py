#!/usr/bin/env python3
import typer
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

app = typer.Typer()
console = Console()


@app.command()
def check(
    bids_dir: Path = typer.Option(..., "--bids", "-b"),
):
    """Check if fMRIPrep outputs match BIDS retinotopy data."""
    
    console.print("[cyan]Scanning BIDS directory for all functional tasks...[/cyan]")
    fmriprep_dir = bids_dir / "derivatives" / "fmriprep" / "analysis-25.1.4"
    # Expected fMRIPrep outputs
    required_files = [
        'desc-brain_mask.nii.gz',
        'desc-confounds_timeseries.json',
        'desc-confounds_timeseries.tsv',
        'desc-preproc_bold.json',
        'desc-preproc_bold.nii.gz',
        'hemi-L_space-fsaverage_bold.func.gii',
        'hemi-L_space-fsaverage_bold.json',
        'hemi-L_space-fsnative_bold.func.gii',
        'hemi-L_space-fsnative_bold.json',
        'hemi-R_space-fsaverage_bold.func.gii',
        'hemi-R_space-fsaverage_bold.json',
        'hemi-R_space-fsnative_bold.func.gii',
        'hemi-R_space-fsnative_bold.json',
    ]
    
    # Find all retinotopy runs in BIDS
    bids_files = list(bids_dir.glob("sub-*/ses-*/func/*task-*_bold.nii.gz"))
    
    console.print(f"[cyan]Found {len(bids_files)} retinotopy runs in BIDS[/cyan]\n")
    
    mismatches = []
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        console=console
    ) as progress:
        
        progress_task = progress.add_task("Checking fMRIPrep outputs...", total=len(bids_files))
        
        for bids_file in bids_files:
            # Parse filename: sub-XX_ses-XX_task-XXX_run-X_bold.nii.gz
            parts = bids_file.stem.replace('.nii', '').split('_')
            sub = ses = task = run = None
            
            for part in parts:
                if part.startswith('sub-'):
                    sub = part
                elif part.startswith('ses-'):
                    ses = part
                elif part.startswith('task-'):
                    task = part.replace('task-', '')
                elif part.startswith('run-'):
                    run = part.replace('run-', '')
            
            if not all([sub, ses, task, run]):
                continue
            
            # Check each required fMRIPrep output
            for req_file in required_files:
                pattern = f"{sub}_{ses}_task-{task}_run-{run}_{req_file}"
                fmri_file = fmriprep_dir / sub / ses / "func" / pattern
                
                if not fmri_file.exists():
                    #console.print(f"[yellow]Looking for: {fmri_file}[/yellow]")
                    mismatches.append((sub, ses, task, run, req_file, "Missing"))
            
            progress.update(progress_task, advance=1)
    
    # Display results
    if mismatches:
        console.print(f"\n[red]Found {len(mismatches)} missing fMRIPrep files:[/red]\n")
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Subject")
        table.add_column("Session")
        table.add_column("Task")
        table.add_column("Run")
        table.add_column("Missing File")
        table.add_column("Status")
        
        for sub, ses, task, run, file, status in mismatches:
            table.add_row(sub, ses, task, run, file, status)
        
        console.print(table)
        
        with open('fmriprep_missing.txt', 'w') as f:
            for sub, ses, task, run, file, status in mismatches:
                f.write(f"{sub}\t{ses}\t{task}\t{run}\t{file}\t{status}\n")
        
        console.print(f"\n[cyan]Saved to fmriprep_missing.txt[/cyan]")
        # Get unique sessions with missing files
        missing_sessions = set((sub, ses) for sub, ses, _, _, _, _ in mismatches)
        
        console.print(f"\n[yellow]Sessions with missing files ({len(missing_sessions)} total):[/yellow]")
        for sub, ses in sorted(missing_sessions):
            console.print(f"  {sub} {ses}")        
    else:
        console.print("\n[green]✅ All fMRIPrep outputs present![/green]")


if __name__ == "__main__":
    app()