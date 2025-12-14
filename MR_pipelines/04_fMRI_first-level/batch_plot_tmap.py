'''
This code is for batch plotting the tmaps, 
stored in the l1_surface folder for quick checking
'''

from nilearn import datasets
from nibabel.freesurfer import read_annot
import pandas as pd
import os
import matplotlib.pyplot as plt
import nibabel.freesurfer as fs
import nibabel as nib
from nilearn import plotting
import numpy as np
import nilearn.surface as surf
from nilearn.surface import load_surf_data
from pathlib import Path
import typer
from typing import Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import matplotlib
matplotlib.use('Agg')

app = typer.Typer()


def plot_tmap_surface_lateral(tmap_file,
                    hemi,
                    path_to_sub_fs, 
                    out_dir,
                    threshold=0,
                    path_to_ROI=None,
                    elev=-30,
                    azimuth=180,
                    vmin=0,
                    vmax=20):
    """
    Plot a single t-map surface visualization - LATERAL view.
    Returns: (tmap_file, output_file, success)
    """
    try:
        # tmap_file parse name
        name = tmap_file.name.replace('.func.gii', '')
        
        ## below is the plot
        surf_data = load_surf_data(tmap_file)
        if hemi == 'L':
            # surf_mesh
            fs_inflated = f'{path_to_sub_fs}/surf/lh.inflated'
            # background
            fs_curv = fs.read_morph_data(f'{path_to_sub_fs}/surf/lh.curv')
            fs_curv_sign = np.sign(fs_curv)
            fs_sulc = fs.read_morph_data(f'{path_to_sub_fs}/surf/lh.sulc')
        else:
            # surf_mesh
            fs_inflated = f'{path_to_sub_fs}/surf/rh.inflated'
            # background
            fs_curv = fs.read_morph_data(f'{path_to_sub_fs}/surf/rh.curv')
            fs_curv_sign = np.sign(fs_curv)
            fs_sulc = fs.read_morph_data(f'{path_to_sub_fs}/surf/rh.sulc')

        # load ROI to plot as contours
        if path_to_ROI:
            votc_annot = fs.read_annot(path_to_ROI)

        # title
        title = f"{name}"

        # set view point
        elva, azimuth_angle = elev, azimuth

        # storage setting
        if threshold > 0:
            # filename
            figure_name = f'lateral_{name}_Tthresh-{threshold}.png'
            output_file = os.path.join(out_dir, figure_name)
            fig = plotting.plot_surf_stat_map(
                fs_inflated, 
                surf_data,
                vmin=threshold, vmax=vmax,
                bg_map=fs_curv_sign, bg_on_data=True,
                cmap='jet', colorbar=True,
                symmetric_cbar=False,
                threshold=threshold,
                darkness=.5, 
                view=(elva, azimuth_angle),
                engine='matplotlib', 
            )
        else:
            figure_name = f'lateral_{name}_orig.png'
            output_file = os.path.join(out_dir, figure_name)
            fig = plotting.plot_surf_stat_map(
                fs_inflated, 
                surf_data,
                vmin=vmin, vmax=vmax,
                bg_map=fs_curv_sign, bg_on_data=True,
                cmap='jet', colorbar=True,
                symmetric_cbar=False,
                darkness=.5, 
                threshold=0.01,
                view=(elva, azimuth_angle),
                engine='matplotlib', 
            )

        fig.savefig(output_file, dpi=150, bbox_inches='tight')
        plt.close(fig)
        
        return (str(tmap_file), str(output_file), True)
    
    except Exception as e:
        return (str(tmap_file), str(e), False)


def plot_tmap_surface_basal(tmap_file, 
                    hemi,
                    path_to_sub_fs, 
                    out_dir,
                    threshold=0,
                    path_to_ROI=None,
                    elev=-90,
                    azimuth=180,
                    vmin=0,
                    vmax=20):
    """
    Plot a single t-map surface visualization - BASAL view.
    Returns: (tmap_file, output_file, success)
    """
    try:
        # tmap_file parse name
        name = tmap_file.name.replace('.func.gii', '')
        
        ## below is the plot
        surf_data = load_surf_data(tmap_file)

        if hemi == 'L':
            # surf_mesh
            fs_inflated = f'{path_to_sub_fs}/surf/lh.inflated'
            # background
            fs_curv = fs.read_morph_data(f'{path_to_sub_fs}/surf/lh.curv')
            fs_curv_sign = np.sign(fs_curv)
            fs_sulc = fs.read_morph_data(f'{path_to_sub_fs}/surf/lh.sulc')
        else:
            # surf_mesh
            fs_inflated = f'{path_to_sub_fs}/surf/rh.inflated'
            # background
            fs_curv = fs.read_morph_data(f'{path_to_sub_fs}/surf/rh.curv')
            fs_curv_sign = np.sign(fs_curv)
            fs_sulc = fs.read_morph_data(f'{path_to_sub_fs}/surf/rh.sulc')

        # load ROI to plot as contours
        if path_to_ROI:
            votc_annot = fs.read_annot(path_to_ROI)

        # title
        title = f"{name}"

        # set view point
        elva, azimuth_angle = elev, azimuth

        # storage setting
        if threshold > 0:
            # filename
            figure_name = f'basal_{name}_Tthresh-{threshold}.png'
            output_file = os.path.join(out_dir, figure_name)
            fig = plotting.plot_surf_stat_map(
                fs_inflated, 
                surf_data,
                vmin=threshold, vmax=vmax,
                bg_map=fs_curv_sign, bg_on_data=True,
                cmap='jet', colorbar=True,
                symmetric_cbar=False,
                threshold=threshold,
                darkness=.5, 
                view=(elva, azimuth_angle),
                engine='matplotlib', 
            )
        else:
            figure_name = f'basal_{name}_orig.png'
            output_file = os.path.join(out_dir, figure_name)
            fig = plotting.plot_surf_stat_map(
                fs_inflated, 
                surf_data,
                vmin=vmin, vmax=vmax,
                bg_map=fs_curv_sign, bg_on_data=True,
                cmap='jet', colorbar=True,
                symmetric_cbar=False,
                threshold=0.01,
                darkness=.5, 
                view=(elva, azimuth_angle),
                engine='matplotlib', 
            )

        fig.savefig(output_file, dpi=150, bbox_inches='tight')
        plt.close(fig)
        
        return (str(tmap_file), str(output_file), True)
    
    except Exception as e:
        return (str(tmap_file), str(e), False)


def plot_both_views(tmap_file, hemi, path_to_sub_fs, out_dir, threshold, lateral_kwargs, basal_kwargs):
    """
    Plot both lateral and basal views for a single t-map.
    Returns: (tmap_file, results_dict, success)
    """
    results = {'lateral': None, 'basal': None}
    all_success = True
    
    # Plot lateral view
    lateral_result = plot_tmap_surface_lateral(
        tmap_file, hemi, path_to_sub_fs, out_dir, threshold, **lateral_kwargs
    )
    results['lateral'] = lateral_result
    if not lateral_result[2]:
        all_success = False
    
    # Plot basal view
    basal_result = plot_tmap_surface_basal(
        tmap_file, hemi, path_to_sub_fs, out_dir, threshold, **basal_kwargs
    )
    results['basal'] = basal_result
    if not basal_result[2]:
        all_success = False
    
    return (str(tmap_file), results, all_success)


def process_single_job(job_args):
    """
    Wrapper function for processing a single job (for parallel execution).
    """
    tmap_file, hemi, path_to_sub_fs, out_dir, threshold, lateral_kwargs, basal_kwargs = job_args
    return plot_both_views(tmap_file, hemi, path_to_sub_fs, out_dir, threshold, lateral_kwargs, basal_kwargs)


@app.command()
def main(
    l1_surface_dir: Path = typer.Option(
        Path('/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS/derivatives/l1_surface/analysis-final_v2'),
        help="Path to l1_surface analysis directory"
    ),
    subses_list_file: Path = typer.Option(
        Path('/bcbl/home/public/Gari/VOTCLOC/main_exp/code/subseslist_floc_run.txt'),
        help="Path to subject-session list CSV/TXT file"
    ),
    fs_dir: Path = typer.Option(
        Path('/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS/derivatives/freesurfer-with_t2'),
        help="Path to FreeSurfer derivatives directory"
    ),
    thresholds: str = typer.Option(
        "0,2.3,3.1",
        help="Comma-separated threshold values"
    ),
    n_workers: int = typer.Option(
        35,
        help="Number of parallel workers"
    ),
    hemisphere: str = typer.Option(
        "R",
        help="Hemisphere to process (L or R)"
    ),
    contrast_filter: str = typer.Option(
        "RWvsPER",
        help="Contrast name filter"
    ),
    lateral_elev: float = typer.Option(
        -30,
        help="Elevation angle for lateral view"
    ),
    lateral_azimuth: float = typer.Option(
        180,
        help="Azimuth angle for lateral view"
    ),
    basal_elev: float = typer.Option(
        -90,
        help="Elevation angle for basal view"
    ),
    basal_azimuth: float = typer.Option(
        180,
        help="Azimuth angle for basal view"
    ),
    vmin: float = typer.Option(
        0,
        help="Minimum value for colorbar"
    ),
    vmax: float = typer.Option(
        15,
        help="Maximum value for colorbar"
    ),
    dry_run: bool = typer.Option(
        False,
        help="Dry run - only show what would be processed"
    )
):
    '''
    This function is used to get the tmap files from l1_surface folder
    and plot them in parallel (both lateral and basal views).
    '''
    
    typer.echo("="*80)
    typer.echo("T-MAP SURFACE PLOTTING - PARALLEL PROCESSING (LATERAL + BASAL)")
    typer.echo("="*80)
    typer.echo(f"L1 Surface Directory: {l1_surface_dir}")
    typer.echo(f"Subject-Session List: {subses_list_file}")
    typer.echo(f"FreeSurfer Directory: {fs_dir}")
    typer.echo(f"Number of Workers: {n_workers}")
    typer.echo(f"Hemisphere: {hemisphere}")
    typer.echo(f"Contrast Filter: {contrast_filter}")
    typer.echo(f"Lateral View Angles: elevation={lateral_elev}, azimuth={lateral_azimuth}")
    typer.echo(f"Basal View Angles: elevation={basal_elev}, azimuth={basal_azimuth}")
    typer.echo("="*80)
    
    # Parse thresholds
    threshold_list = [float(t.strip()) for t in thresholds.split(',')]
    typer.echo(f"Thresholds to process: {threshold_list}")
    
    # Read subject-session list
    subses_list = pd.read_csv(subses_list_file)
    typer.echo(f"Loaded {len(subses_list)} subjects/sessions from list")
    
    # Prepare all jobs
    all_jobs = []

    for index, row in subses_list.iterrows():
        sub = f"{row['sub']:02d}"
        ses = f"{row['ses']:02d}"
        RUN = row.get('RUN', True)
        
        if not RUN:
            continue
            
        typer.echo(f'Preparing jobs for sub-{sub}, ses-{ses}')
        
        path_to_sub_fs = fs_dir / f'sub-{sub}'
        
        # Define output directory
        sub_ses_dir = l1_surface_dir / f'sub-{sub}' / f'ses-{ses}' 
        
        # Check if directories exist
        if not path_to_sub_fs.exists():
            typer.secho(f"⚠ FreeSurfer directory not found: {path_to_sub_fs}", fg=typer.colors.YELLOW)
            continue
        
        if not sub_ses_dir.exists():
            typer.secho(f"⚠ L1 surface directory not found: {sub_ses_dir}", fg=typer.colors.YELLOW)
            continue

        # Get tmap files
        tmap_files = list(sub_ses_dir.glob('*stat-t_statmap.func.gii'))
        tmap_files = [f for f in tmap_files 
                    if f'hemi-{hemisphere}' in f.name and contrast_filter in f.name]
        
        typer.echo(f"  Found {len(tmap_files)} t-map files")
        
        for threshold in threshold_list:
            out_dir = l1_surface_dir / f'sub-{sub}' / f'tmap_plots_thresh-{threshold}'
            out_dir.mkdir(parents=True, exist_ok=True)
            for tmap_file in tmap_files:
                # Lateral view kwargs
                lateral_kwargs = {
                    'path_to_ROI': None,
                    'elev': lateral_elev,
                    'azimuth': lateral_azimuth,
                    'vmin': vmin,
                    'vmax': vmax
                }
                
                # Basal view kwargs
                basal_kwargs = {
                    'path_to_ROI': None,
                    'elev': basal_elev,
                    'azimuth': basal_azimuth,
                    'vmin': vmin,
                    'vmax': vmax
                }
                
                job_args = (
                    tmap_file,
                    hemisphere,
                    path_to_sub_fs,
                    out_dir,
                    threshold,
                    lateral_kwargs,
                    basal_kwargs
                )
                all_jobs.append(job_args)
    
    typer.echo("="*80)
    typer.secho(f"TOTAL JOBS TO PROCESS: {len(all_jobs)} (each creates 2 plots)", 
                fg=typer.colors.BRIGHT_CYAN, bold=True)
    typer.secho(f"TOTAL PLOTS TO CREATE: {len(all_jobs) * 2}", 
                fg=typer.colors.BRIGHT_CYAN, bold=True)
    typer.echo("="*80)
    
    if dry_run:
        typer.secho("DRY RUN - No plots will be generated", fg=typer.colors.YELLOW)
        typer.echo(f"Would process {len(all_jobs)} jobs ({len(all_jobs)*2} plots) with {n_workers} workers")
        return
    
    if len(all_jobs) == 0:
        typer.secho("⚠ No jobs to process!", fg=typer.colors.YELLOW)
        return
    
    # Process jobs in parallel
    typer.echo(f"Starting parallel processing with {n_workers} workers...")
    typer.echo("Each job creates BOTH lateral and basal views")
    
    success_count = 0
    error_count = 0
    errors = []
    lateral_success = 0
    basal_success = 0
    
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        # Submit all jobs
        futures = {executor.submit(process_single_job, job): job for job in all_jobs}
        
        # Process completed jobs with progress bar
        with tqdm(total=len(all_jobs), desc="Plotting t-maps (2 views each)", unit="job") as pbar:
            for future in as_completed(futures):
                job = futures[future]
                try:
                    tmap_file, results, success = future.result()
                    
                    if success:
                        success_count += 1
                    else:
                        error_count += 1
                    
                    # Track individual view success
                    if results['lateral'][2]:
                        lateral_success += 1
                    else:
                        error_msg = f"Lateral view failed: {Path(tmap_file).name} - {results['lateral'][1]}"
                        errors.append(error_msg)
                    
                    if results['basal'][2]:
                        basal_success += 1
                    else:
                        error_msg = f"Basal view failed: {Path(tmap_file).name} - {results['basal'][1]}"
                        errors.append(error_msg)
                        
                except Exception as e:
                    error_count += 1
                    error_msg = f"Exception processing job: {e}"
                    errors.append(error_msg)
                
                pbar.update(1)
    
    # Final summary
    typer.echo("")
    typer.echo("="*80)
    typer.secho("PROCESSING COMPLETE", fg=typer.colors.BRIGHT_GREEN, bold=True)
    typer.echo("="*80)
    typer.echo(f"Total jobs: {len(all_jobs)}")
    typer.secho(f"✓ Jobs fully successful (both views): {success_count}", fg=typer.colors.GREEN)
    typer.secho(f"✗ Jobs with errors: {error_count}", 
                fg=typer.colors.RED if error_count > 0 else typer.colors.GREEN)
    typer.echo(f"Job success rate: {success_count/len(all_jobs)*100:.2f}%")
    typer.echo("")
    typer.echo("View-specific results:")
    typer.secho(f"  ✓ Lateral views created: {lateral_success}/{len(all_jobs)}", fg=typer.colors.CYAN)
    typer.secho(f"  ✓ Basal views created: {basal_success}/{len(all_jobs)}", fg=typer.colors.CYAN)
    typer.echo(f"  Total plots created: {lateral_success + basal_success}/{len(all_jobs)*2}")
    
    if errors:
        typer.echo("")
        typer.echo("="*80)
        typer.secho("ERRORS SUMMARY", fg=typer.colors.RED)
        typer.echo("="*80)
        for error in errors[:20]:  # Show first 20 errors
            typer.secho(f"  • {error}", fg=typer.colors.RED)
        if len(errors) > 20:
            typer.echo(f"  ... and {len(errors)-20} more errors")
    
    typer.echo("="*80)


if __name__ == "__main__":
    app()