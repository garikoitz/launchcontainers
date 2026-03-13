#!/usr/bin/env python3
import typer
from pathlib import Path
from typing import List, Tuple
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
import scipy.io as sio

app = typer.Typer()
console = Console()


def get_vistadisp_task(mat_file: Path) -> str:
    """Extract task name from vistadisp .mat file."""
    try:
        mat = sio.loadmat(str(mat_file), simplify_cells=True)
        stim_name = mat.get('params', {}).get('stimName') or mat.get('stimName')
        return stim_name if stim_name else None
    except:
        return None


@app.command()
def check(
    basedir: Path = typer.Option("/scratch/tlei/VOTCLOC", "--basedir", "-base"),
    subseslist: Path = typer.Option("/scratch/tlei/VOTCLOC/code/subseslist_ret_wc_clean.txt", "--subseslist", "-ssl"),
):
    """Check retinotopy task matching between vistadisp and BIDS."""
    
    console.print("[cyan]Reading subject/session list...[/cyan]")
    vistadisp_dir = basedir / "sourcedata" / "vistadisplog"
    bids_dir = basedir / "BIDS"
    fmriprep_dir = bids_dir / "derivatives" / "fmriprep" / "analysis-25.1.4"

    # Expected file patterns for each run
    required_files = [
        'desc-brain_mask.nii.gz',
        'desc-confounds_timeseries.json',
        'desc-confounds_timeseries.tsv',
        'desc-coreg_boldref.json',
        'desc-coreg_boldref.nii.gz',
        'desc-hmc_boldref.json',
        'desc-hmc_boldref.nii.gz',
        'desc-preproc_bold.json',
        'desc-preproc_bold.nii.gz',
        # "from-boldref_to-T1w_mode-image_desc-coreg_xfm.json",
        # "from-boldref_to-T1w_mode-image_desc-coreg_xfm.txt",
        # "from-orig_to-boldref_mode-image_desc-hmc_xfm.json",
        # "from-orig_to-boldref_mode-image_desc-hmc_xfm.txt",
        'hemi-L_space-fsaverage_bold.func.gii',
        'hemi-L_space-fsaverage_bold.json',
        'hemi-L_space-fsnative_bold.func.gii',
        'hemi-L_space-fsnative_bold.json',
        'hemi-R_space-fsaverage_bold.func.gii',
        'hemi-R_space-fsaverage_bold.json',
        'hemi-R_space-fsnative_bold.func.gii',
        'hemi-R_space-fsnative_bold.json',
        # "space-MNI152NLin2009cAsym_boldref.json",
        # "space-MNI152NLin2009cAsym_boldref.nii.gz",
        # "space-MNI152NLin2009cAsym_desc-brain_mask.json",
        # "space-MNI152NLin2009cAsym_desc-brain_mask.nii.gz",
        # "space-MNI152NLin2009cAsym_desc-preproc_bold.json",
        # "space-MNI152NLin2009cAsym_desc-preproc_bold.nii.gz",
        # "space-T1w_boldref.json",
        # "space-T1w_boldref.nii.gz",
        # "space-T1w_desc-brain_mask.json",
        # "space-T1w_desc-brain_mask.nii.gz",
        # "space-T1w_desc-preproc_bold.json",
        # "space-T1w_desc-preproc_bold.nii.gz",
    ]
    with open(subseslist) as f:
        pairs = [line.strip().split(',') for line in f]
    
    mismatches_bids = []
    mismatches_fmriprep = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        console=console
    ) as progress:
        
        task = progress.add_task("Checking tasks...", total=len(pairs))
        
        for sub, ses in pairs:
            # Get vistadisp tasks
            vista_files = list(vistadisp_dir.glob(f"{sub}/{ses}/*task-ret*_params.mat"))
            
            for vista_file in vista_files:
                task_name = get_vistadisp_task(vista_file)
                if not task_name:
                    continue
                
                # Check BIDS
                bids_nii = list(bids_dir.glob(f"{sub}/{ses}/func/*task-{task_name}*_bold.nii.gz"))
                bids_json = list(bids_dir.glob(f"{sub}/{ses}/func/*task-{task_name}*_bold.json"))

                # check fmriprep
                # Extract run number from vistadisp filename
                run_match = vista_file.name.split('run-')[1].split('_')[0] if 'run-' in vista_file.name else None
                if not run_match:
                    continue
                
                # Check each required file
                for req_file in required_files:
                    pattern = f"{sub}_{ses}_task-{task_name}_run-{run_match}_{req_file}"
                    gii_file = fmriprep_dir / sub / ses / "func" / pattern
                                   
                    if not gii_file.exists():
                        mismatches_fmriprep.append((sub, ses, task_name, run_match, req_file.split('_')[0], "Missing file"))

            
 
                
                if not bids_nii:
                    mismatches_bids.append((sub, ses, task_name, "Missing NIfTI"))
                if not bids_json:
                    mismatches_bids.append((sub, ses, task_name, "Missing JSON"))
            
            progress.update(task, advance=1)
    
    # Display results
    if mismatches_bids:
        console.print(f"\n[red]Found {len(mismatches_bids)} mismatches_bids:[/red]\n")
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Subject")
        table.add_column("Session")
        table.add_column("Task")
        table.add_column("Issue")
        
        for sub, ses, task, issue in mismatches_bids:
            table.add_row(sub, ses, task, issue)
        
        console.print(table)
        
        # Save to file
        with open('task_mismatches_bids.txt', 'w') as f:
            for sub, ses, task, issue in mismatches_bids:
                f.write(f"{sub}\t{ses}\t{task}\t{issue}\n")
        
        console.print(f"\n[cyan]Saved to task_mismatches_bids.txt[/cyan]")
    else:
        console.print("\n[green]✅ All tasks match![/green]")
    
    # Display results
    if mismatches_fmriprep:
        console.print(f"\n[red]Found {len(mismatches_fmriprep)} mismatches_fmriprep:[/red]\n")
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Subject")
        table.add_column("Session")
        table.add_column("Task")
        table.add_column("Run")
        table.add_column("File Type")
        table.add_column("Issue")
        
        for sub, ses, task, run, file_type, issue in mismatches_fmriprep:
            table.add_row(sub, ses, task, run, file_type, issue)
        
        console.print(table)
        
        with open('fmriprep_mismatches.txt', 'w') as f:
            for sub, ses, task, run, file_type, issue in mismatches_fmriprep:
                f.write(f"{sub}\t{ses}\t{task}\t{run}\t{file_type}\t{issue}\n")
        
        console.print(f"\n[cyan]Saved to fmriprep_mismatches.txt[/cyan]")
    else:
        console.print("\n[green]✅ All fMRIPrep outputs match![/green]")        


if __name__ == "__main__":
    app()