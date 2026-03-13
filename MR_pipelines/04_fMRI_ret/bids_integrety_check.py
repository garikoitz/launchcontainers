#!/usr/bin/env python3
"""
BIDS Dataset Integrity Checker

Quick scan to detect data loss issues:
1. Missing JSON sidecar files for NIfTI images
2. Missing NIfTI files that have JSON sidecars
3. Corrupted or unreadable NIfTI files
4. Malformed or empty JSON files
5. File size anomalies

Outputs summary statistics of problems found.
"""

import json
import gzip
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
import nibabel as nib
from collections import defaultdict
from multiprocessing import Pool, cpu_count
from functools import partial
import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich import print as rprint

app = typer.Typer(help="Check BIDS dataset integrity - detect data loss")
console = Console()


def check_nifti_wrapper(nii_path: Path) -> Tuple[Path, bool, Optional[str], Optional[Dict]]:
    """
    Wrapper for parallel processing of NIfTI files.
    Returns the path along with check results.
    """
    is_valid, error, info = check_nifti_readable(nii_path)
    return nii_path, is_valid, error, info


def check_json_wrapper(json_path: Path) -> Tuple[Path, bool, Optional[str], Optional[Dict]]:
    """
    Wrapper for parallel processing of JSON files.
    Returns the path along with check results.
    """
    is_valid, error, content = check_json_readable(json_path)
    return json_path, is_valid, error, content


def check_nifti_readable(nii_path: Path) -> Tuple[bool, Optional[str], Optional[Dict]]:
    """
    Check if a NIfTI file is readable and get basic info.
    
    Returns:
        (is_valid, error_message, info_dict)
    """
    try:
        # Try to load the NIfTI file
        img = nib.load(str(nii_path))
        
        # Get basic info
        info = {
            'shape': img.shape,
            'dtype': str(img.get_data_dtype()),
            'size_mb': nii_path.stat().st_size / (1024 * 1024)
        }
        
        # Check if file is actually gzipped (for .nii.gz files)
        if nii_path.suffix == '.gz':
            with gzip.open(nii_path, 'rb') as f:
                # Try to read first few bytes
                f.read(100)
        
        return True, None, info
        
    except gzip.BadGzipFile:
        return False, "Corrupted gzip file", None
    except nib.filebasedimages.ImageFileError as e:
        return False, f"NIfTI read error: {str(e)}", None
    except Exception as e:
        return False, f"Unexpected error: {str(e)}", None


def check_json_readable(json_path: Path) -> Tuple[bool, Optional[str], Optional[Dict]]:
    """
    Check if a JSON file is readable and valid.
    
    Returns:
        (is_valid, error_message, content)
    """
    try:
        # Check if file is zero bytes
        file_size = json_path.stat().st_size
        if file_size == 0:
            return False, "Zero-byte file (completely empty)", None
        
        # Try to read the JSON
        with open(json_path, 'r') as f:
            content = json.load(f)
        
        # Check if content is None or empty
        if content is None:
            return False, "JSON contains null", None
        
        if not content:
            return False, "Empty JSON file (no content)", None
        
        if not isinstance(content, dict):
            return False, "JSON is not a dictionary", None
        
        # Check if it's just an empty dictionary {}
        if len(content) == 0:
            return False, "JSON is empty dictionary {} (no fields)", None
        
        # For BIDS functional data, check for critical fields
        if '_bold.json' in json_path.name or '_task-' in json_path.name:
            critical_fields = ['RepetitionTime', 'TaskName']
            missing_fields = [f for f in critical_fields if f not in content]
            if missing_fields:
                return False, f"Missing critical BIDS fields: {', '.join(missing_fields)}", content
        
        return True, None, content
        
    except json.JSONDecodeError as e:
        return False, f"JSON parsing error: {str(e)}", None
    except Exception as e:
        return False, f"Unexpected error: {str(e)}", None


def find_bids_imaging_files(bids_dir: Path, 
                            modalities: List[str] = None,
                            is_fmriprep: bool = False) -> Dict[str, List[Path]]:
    """
    Find all imaging files in a BIDS dataset or fMRIPrep derivatives.
    
    Args:
        bids_dir: Path to BIDS dataset root or fMRIPrep derivatives
        modalities: List of modalities to check (e.g., ['func', 'anat'])
                   If None, checks all
        is_fmriprep: If True, look for fMRIPrep preprocessed files (*preproc*, *space-*)
    
    Returns:
        Dictionary mapping file type to list of paths
    """
    if modalities is None:
        modalities = ['func', 'anat', 'fmap', 'dwi']
    
    files = {
        'nifti': [],
        'json': []
    }
    
    for subject_dir in sorted(bids_dir.glob("sub-*")):
        if not subject_dir.is_dir():
            continue
        
        for session_dir in sorted(list(subject_dir.glob("ses-*")) + [subject_dir]):
            if not session_dir.is_dir():
                continue
            
            for modality in modalities:
                mod_dir = session_dir / modality
                if not mod_dir.exists():
                    continue
                
                if is_fmriprep:
                    # For fMRIPrep, look for preprocessed files
                    # Functional: *_space-*_desc-preproc_bold.nii.gz
                    # Anatomical: *_space-*_desc-preproc_T1w.nii.gz, etc.
                    for nii_file in mod_dir.glob("*_preproc_*.nii.gz"):
                        files['nifti'].append(nii_file)
                    for nii_file in mod_dir.glob("*_space-*.nii.gz"):
                        if nii_file not in files['nifti']:  # Avoid duplicates
                            files['nifti'].append(nii_file)
                    
                    # JSON files for preprocessed data
                    for json_file in mod_dir.glob("*_preproc_*.json"):
                        files['json'].append(json_file)
                    for json_file in mod_dir.glob("*_space-*.json"):
                        if json_file not in files['json']:
                            files['json'].append(json_file)
                else:
                    # For raw BIDS, find regular NIfTI files
                    for nii_file in mod_dir.glob("*.nii.gz"):
                        files['nifti'].append(nii_file)
                    for nii_file in mod_dir.glob("*.nii"):
                        files['nifti'].append(nii_file)
                    
                    # Find JSON files
                    for json_file in mod_dir.glob("*.json"):
                        files['json'].append(json_file)
    
    return files


def get_nifti_json_pairs(bids_dir: Path, 
                         modalities: List[str] = None,
                         is_fmriprep: bool = False) -> Tuple[Dict, Dict, Set]:
    """
    Find all NIfTI-JSON pairs and orphaned files.
    
    Args:
        bids_dir: Path to BIDS dataset root or fMRIPrep derivatives
        modalities: List of modalities to check
        is_fmriprep: If True, check fMRIPrep preprocessed files
    
    Returns:
        (nifti_to_json, json_to_nifti, orphaned_files)
    """
    files = find_bids_imaging_files(bids_dir, modalities, is_fmriprep)
    
    # Create mapping of base names to files
    nifti_by_base = {}
    json_by_base = {}
    
    for nii_path in files['nifti']:
        # Get base name (remove .nii.gz or .nii)
        base = str(nii_path).replace('.nii.gz', '').replace('.nii', '')
        nifti_by_base[base] = nii_path
    
    for json_path in files['json']:
        # Get base name (remove .json)
        base = str(json_path).replace('.json', '')
        json_by_base[base] = json_path
    
    # Find pairs and orphans
    all_bases = set(nifti_by_base.keys()) | set(json_by_base.keys())
    nifti_to_json = {}
    json_to_nifti = {}
    orphaned = set()
    
    for base in all_bases:
        has_nifti = base in nifti_by_base
        has_json = base in json_by_base
        
        if has_nifti and has_json:
            nifti_to_json[nifti_by_base[base]] = json_by_base[base]
            json_to_nifti[json_by_base[base]] = nifti_by_base[base]
        elif has_nifti:
            orphaned.add(('nifti', nifti_by_base[base]))
        elif has_json:
            orphaned.add(('json', json_by_base[base]))
    
    return nifti_to_json, json_to_nifti, orphaned


@app.command()
def check(
    bids_dir: Path = typer.Argument(..., help="Path to BIDS dataset root or fMRIPrep derivatives directory"),
    modalities: Optional[List[str]] = typer.Option(
        None, "--modality", "-m",
        help="Modalities to check (func, anat, fmap, dwi). If not specified, checks all"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v",
        help="Show detailed error messages for each problematic file"
    ),
    output: Optional[Path] = typer.Option(
        None, "--output", "-o",
        help="Save detailed results to JSON file"
    ),
    broken_files_list: Optional[Path] = typer.Option(
        None, "--broken-list", "-b",
        help="Save list of broken file paths to text file (one path per line)"
    ),
    n_jobs: int = typer.Option(
        15, "--n-jobs", "-j",
        help="Number of parallel jobs (default: 15)"
    ),
    is_fmriprep: bool = typer.Option(
        False, "--fmriprep", "-f",
        help="Check fMRIPrep derivatives directory (looks for *preproc* files)"
    ),
):
    """
    Check BIDS dataset integrity and report summary of problems.
    Can check both raw BIDS data and fMRIPrep derivatives.
    Uses parallel processing with specified number of cores.
    """
    
    console.print(Panel.fit(
        f"[bold]BIDS Dataset Integrity Check[/bold]\n{bids_dir}\n[cyan]Using {n_jobs} parallel jobs[/cyan]",
        border_style="blue"
    ))
    
    # Statistics
    stats = {
        'total_nifti': 0,
        'total_json': 0,
        'valid_nifti': 0,
        'valid_json': 0,
        'corrupted_nifti': 0,
        'corrupted_json': 0,
        'missing_json': 0,
        'missing_nifti': 0,
        'suspicious_sizes': 0,
    }
    
    problems = {
        'corrupted_nifti': [],
        'corrupted_json': [],
        'missing_json': [],
        'missing_nifti': [],
        'suspicious_sizes': [],
    }
    
    # Step 1: Find all files and check for pairs
    console.print("\n[cyan]Step 1: Scanning for files and checking pairs...[/cyan]")
    
    nifti_to_json, json_to_nifti, orphaned = get_nifti_json_pairs(bids_dir, modalities, is_fmriprep)
    
    stats['total_nifti'] = len(nifti_to_json) + len([x for x in orphaned if x[0] == 'nifti'])
    stats['total_json'] = len(json_to_nifti) + len([x for x in orphaned if x[0] == 'json'])
    
    # Report orphaned files
    for file_type, file_path in orphaned:
        if file_type == 'nifti':
            stats['missing_json'] += 1
            problems['missing_json'].append(file_path)
        else:  # json
            stats['missing_nifti'] += 1
            problems['missing_nifti'].append(file_path)
    
    console.print(f"  Found {stats['total_nifti']} NIfTI files")
    console.print(f"  Found {stats['total_json']} JSON files")
    console.print(f"  Found {len(orphaned)} orphaned files (missing pair)")
    
    # Step 2: Check NIfTI file integrity (PARALLEL)
    console.print(f"\n[cyan]Step 2: Checking NIfTI file integrity (using {n_jobs} cores)...[/cyan]")
    
    nifti_files = list(nifti_to_json.keys())
    nifti_sizes = []
    
    if nifti_files:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            
            task = progress.add_task("Checking NIfTI files...", total=len(nifti_files))
            
            # Parallel processing
            with Pool(processes=n_jobs) as pool:
                for nii_path, is_valid, error, info in pool.imap_unordered(check_nifti_wrapper, nifti_files):
                    if is_valid:
                        stats['valid_nifti'] += 1
                        nifti_sizes.append(info['size_mb'])
                    else:
                        stats['corrupted_nifti'] += 1
                        problems['corrupted_nifti'].append((nii_path, error))
                    
                    progress.update(task, advance=1)
    
    # Check for suspicious file sizes (potential incomplete transfers)
    if nifti_sizes:
        import numpy as np
        median_size = np.median(nifti_sizes)
        std_size = np.std(nifti_sizes)
        
        for nii_path, json_path in nifti_to_json.items():
            size_mb = nii_path.stat().st_size / (1024 * 1024)
            # Flag files that are suspiciously small (< 50% of median)
            if size_mb < median_size * 0.5 and median_size > 10:
                stats['suspicious_sizes'] += 1
                problems['suspicious_sizes'].append((nii_path, size_mb, median_size))
    
    # Step 3: Check JSON file integrity (PARALLEL)
    console.print(f"\n[cyan]Step 3: Checking JSON file integrity (using {n_jobs} cores)...[/cyan]")
    
    json_files = list(json_to_nifti.keys())
    
    if json_files:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            
            task = progress.add_task("Checking JSON files...", total=len(json_files))
            
            # Parallel processing
            with Pool(processes=n_jobs) as pool:
                for json_path, is_valid, error, content in pool.imap_unordered(check_json_wrapper, json_files):
                    if is_valid:
                        stats['valid_json'] += 1
                    else:
                        stats['corrupted_json'] += 1
                        problems['corrupted_json'].append((json_path, error))
                    
                    progress.update(task, advance=1)
    
    # Display Summary
    console.print("\n" + "="*70)
    console.print("[bold]SUMMARY OF PROBLEMS[/bold]")
    console.print("="*70)
    
    total_problems = (stats['corrupted_nifti'] + stats['corrupted_json'] + 
                     stats['missing_json'] + stats['missing_nifti'] + 
                     stats['suspicious_sizes'])
    
    if total_problems == 0:
        console.print("\n[bold green]✅ No problems found! Dataset appears healthy.[/bold green]")
    else:
        console.print(f"\n[bold red]⚠️  Found {total_problems} total problems[/bold red]\n")
        
        # Create summary table
        table = Table(show_header=True, header_style="bold yellow")
        table.add_column("Issue Type", style="cyan")
        table.add_column("Count", justify="right", style="magenta")
        table.add_column("Status", justify="center")
        
        def add_row(label, count, total):
            if count == 0:
                status = "[green]✅[/green]"
            elif count < total * 0.1:
                status = "[yellow]⚠️[/yellow]"
            else:
                status = "[red]❌[/red]"
            table.add_row(label, str(count), status)
        
        add_row("Corrupted NIfTI files", stats['corrupted_nifti'], stats['total_nifti'])
        add_row("Corrupted JSON files", stats['corrupted_json'], stats['total_json'])
        add_row("Missing JSON sidecars", stats['missing_json'], stats['total_nifti'])
        add_row("Orphaned JSON files", stats['missing_nifti'], stats['total_json'])
        add_row("Suspicious file sizes", stats['suspicious_sizes'], stats['total_nifti'])
        
        console.print(table)
    
    # Show statistics
    console.print(f"\n[bold]Dataset Statistics:[/bold]")
    console.print(f"  Total NIfTI files: {stats['total_nifti']}")
    console.print(f"  Valid NIfTI files: {stats['valid_nifti']} ([green]{stats['valid_nifti']/stats['total_nifti']*100:.1f}%[/green])")
    console.print(f"  Total JSON files: {stats['total_json']}")
    console.print(f"  Valid JSON files: {stats['valid_json']} ([green]{stats['valid_json']/stats['total_json']*100:.1f}%[/green])" if stats['total_json'] > 0 else "  Valid JSON files: 0")
    
    # Detailed problem listing
    if verbose and total_problems > 0:
        console.print("\n" + "="*70)
        console.print("[bold]DETAILED PROBLEM LIST[/bold]")
        console.print("="*70)
        
        if problems['corrupted_nifti']:
            console.print("\n[bold red]Corrupted NIfTI Files:[/bold red]")
            for nii_path, error in problems['corrupted_nifti']:
                console.print(f"  [red]❌ {nii_path.relative_to(bids_dir)}[/red]")
                console.print(f"     Error: {error}")
        
        if problems['corrupted_json']:
            console.print("\n[bold red]Corrupted JSON Files:[/bold red]")
            for json_path, error in problems['corrupted_json']:
                console.print(f"  [red]❌ {json_path.relative_to(bids_dir)}[/red]")
                console.print(f"     Error: {error}")
        
        if problems['missing_json']:
            console.print("\n[bold yellow]Missing JSON Sidecars:[/bold yellow]")
            for nii_path in problems['missing_json']:
                console.print(f"  [yellow]⚠️  {nii_path.relative_to(bids_dir)}[/yellow]")
        
        if problems['missing_nifti']:
            console.print("\n[bold yellow]Orphaned JSON Files (no corresponding NIfTI):[/bold yellow]")
            for json_path in problems['missing_nifti']:
                console.print(f"  [yellow]⚠️  {json_path.relative_to(bids_dir)}[/yellow]")
        
        if problems['suspicious_sizes']:
            console.print("\n[bold yellow]Suspicious File Sizes (potential incomplete transfer):[/bold yellow]")
            for nii_path, size_mb, median_mb in problems['suspicious_sizes']:
                console.print(f"  [yellow]⚠️  {nii_path.relative_to(bids_dir)}[/yellow]")
                console.print(f"     Size: {size_mb:.1f} MB (median: {median_mb:.1f} MB)")
    
    # Save broken files list to text file
    if broken_files_list and total_problems > 0:
        console.print(f"\n[cyan]Saving broken files list to {broken_files_list}...[/cyan]")
        
        with open(broken_files_list, 'w') as f:
            # Write header
            f.write(f"# BIDS Dataset Integrity Check - Broken Files\n")
            f.write(f"# Dataset: {bids_dir}\n")
            f.write(f"# Total problems: {total_problems}\n")
            f.write(f"# Generated: {Path.cwd()}\n\n")
            
            # Write corrupted NIfTI files
            if problems['corrupted_nifti']:
                f.write("# Corrupted NIfTI Files\n")
                for nii_path, error in problems['corrupted_nifti']:
                    f.write(f"{nii_path}\n")
                f.write("\n")
            
            # Write corrupted JSON files
            if problems['corrupted_json']:
                f.write("# Corrupted JSON Files\n")
                for json_path, error in problems['corrupted_json']:
                    f.write(f"{json_path}\n")
                f.write("\n")
            
            # Write missing JSON files
            if problems['missing_json']:
                f.write("# NIfTI Files Missing JSON Sidecars\n")
                for nii_path in problems['missing_json']:
                    f.write(f"{nii_path}\n")
                f.write("\n")
            
            # Write orphaned JSON files
            if problems['missing_nifti']:
                f.write("# Orphaned JSON Files (No Corresponding NIfTI)\n")
                for json_path in problems['missing_nifti']:
                    f.write(f"{json_path}\n")
                f.write("\n")
            
            # Write suspicious size files
            if problems['suspicious_sizes']:
                f.write("# Files with Suspicious Sizes\n")
                for nii_path, size_mb, median_mb in problems['suspicious_sizes']:
                    f.write(f"{nii_path}\n")
                f.write("\n")
        
        console.print(f"[green]Broken files list saved to {broken_files_list}[/green]")
        console.print(f"[green]Total broken files: {total_problems}[/green]")
    
    # Save to JSON file if requested
    if output:
        console.print(f"\n[cyan]Saving detailed report to {output}...[/cyan]")
        
        report = {
            'summary': stats,
            'problems': {
                'corrupted_nifti': [str(p[0]) for p in problems['corrupted_nifti']],
                'corrupted_json': [str(p[0]) for p in problems['corrupted_json']],
                'missing_json': [str(p) for p in problems['missing_json']],
                'missing_nifti': [str(p) for p in problems['missing_nifti']],
                'suspicious_sizes': [str(p[0]) for p in problems['suspicious_sizes']],
            }
        }
        
        with open(output, 'w') as f:
            json.dump(report, f, indent=2)
        
        console.print(f"[green]Report saved to {output}[/green]")
    
    # Return exit code based on problems found
    if stats['corrupted_nifti'] > 0 or stats['corrupted_json'] > 0:
        console.print("\n[bold red]⚠️  Critical problems found - some files are corrupted![/bold red]")
        return 1
    elif total_problems > 0:
        console.print("\n[bold yellow]⚠️  Some issues found - review the problems above[/bold yellow]")
        return 0
    else:
        return 0
    """
    Check BIDS dataset integrity and report summary of problems.
    Can check both raw BIDS data and fMRIPrep derivatives.
    Uses parallel processing with specified number of cores.
    """
    
    console.print(Panel.fit(
        f"[bold]BIDS Dataset Integrity Check[/bold]\n{bids_dir}\n[cyan]Using {n_jobs} parallel jobs[/cyan]",
        border_style="blue"
    ))
    
    # Statistics
    stats = {
        'total_nifti': 0,
        'total_json': 0,
        'valid_nifti': 0,
        'valid_json': 0,
        'corrupted_nifti': 0,
        'corrupted_json': 0,
        'missing_json': 0,
        'missing_nifti': 0,
        'suspicious_sizes': 0,
    }
    
    problems = {
        'corrupted_nifti': [],
        'corrupted_json': [],
        'missing_json': [],
        'missing_nifti': [],
        'suspicious_sizes': [],
    }
    
    # Step 1: Find all files and check for pairs
    console.print("\n[cyan]Step 1: Scanning for files and checking pairs...[/cyan]")
    
    nifti_to_json, json_to_nifti, orphaned = get_nifti_json_pairs(bids_dir, modalities, is_fmriprep)
    
    stats['total_nifti'] = len(nifti_to_json) + len([x for x in orphaned if x[0] == 'nifti'])
    stats['total_json'] = len(json_to_nifti) + len([x for x in orphaned if x[0] == 'json'])
    
    # Report orphaned files
    for file_type, file_path in orphaned:
        if file_type == 'nifti':
            stats['missing_json'] += 1
            problems['missing_json'].append(file_path)
        else:  # json
            stats['missing_nifti'] += 1
            problems['missing_nifti'].append(file_path)
    
    console.print(f"  Found {stats['total_nifti']} NIfTI files")
    console.print(f"  Found {stats['total_json']} JSON files")
    console.print(f"  Found {len(orphaned)} orphaned files (missing pair)")
    
    # Step 2: Check NIfTI file integrity (PARALLEL)
    console.print(f"\n[cyan]Step 2: Checking NIfTI file integrity (using {n_jobs} cores)...[/cyan]")
    
    nifti_files = list(nifti_to_json.keys())
    nifti_sizes = []
    
    if nifti_files:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            
            task = progress.add_task("Checking NIfTI files...", total=len(nifti_files))
            
            # Parallel processing
            with Pool(processes=n_jobs) as pool:
                for nii_path, is_valid, error, info in pool.imap_unordered(check_nifti_wrapper, nifti_files):
                    if is_valid:
                        stats['valid_nifti'] += 1
                        nifti_sizes.append(info['size_mb'])
                    else:
                        stats['corrupted_nifti'] += 1
                        problems['corrupted_nifti'].append((nii_path, error))
                    
                    progress.update(task, advance=1)
    
    # Check for suspicious file sizes (potential incomplete transfers)
    if nifti_sizes:
        import numpy as np
        median_size = np.median(nifti_sizes)
        std_size = np.std(nifti_sizes)
        
        for nii_path, json_path in nifti_to_json.items():
            size_mb = nii_path.stat().st_size / (1024 * 1024)
            # Flag files that are suspiciously small (< 50% of median)
            if size_mb < median_size * 0.5 and median_size > 10:
                stats['suspicious_sizes'] += 1
                problems['suspicious_sizes'].append((nii_path, size_mb, median_size))
    
    # Step 3: Check JSON file integrity (PARALLEL)
    console.print(f"\n[cyan]Step 3: Checking JSON file integrity (using {n_jobs} cores)...[/cyan]")
    
    json_files = list(json_to_nifti.keys())
    
    if json_files:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            
            task = progress.add_task("Checking JSON files...", total=len(json_files))
            
            # Parallel processing
            with Pool(processes=n_jobs) as pool:
                for json_path, is_valid, error, content in pool.imap_unordered(check_json_wrapper, json_files):
                    if is_valid:
                        stats['valid_json'] += 1
                    else:
                        stats['corrupted_json'] += 1
                        problems['corrupted_json'].append((json_path, error))
                    
                    progress.update(task, advance=1)
    
    # Display Summary
    console.print("\n" + "="*70)
    console.print("[bold]SUMMARY OF PROBLEMS[/bold]")
    console.print("="*70)
    
    total_problems = (stats['corrupted_nifti'] + stats['corrupted_json'] + 
                     stats['missing_json'] + stats['missing_nifti'] + 
                     stats['suspicious_sizes'])
    
    if total_problems == 0:
        console.print("\n[bold green]✅ No problems found! Dataset appears healthy.[/bold green]")
    else:
        console.print(f"\n[bold red]⚠️  Found {total_problems} total problems[/bold red]\n")
        
        # Create summary table
        table = Table(show_header=True, header_style="bold yellow")
        table.add_column("Issue Type", style="cyan")
        table.add_column("Count", justify="right", style="magenta")
        table.add_column("Status", justify="center")
        
        def add_row(label, count, total):
            if count == 0:
                status = "[green]✅[/green]"
            elif count < total * 0.1:
                status = "[yellow]⚠️[/yellow]"
            else:
                status = "[red]❌[/red]"
            table.add_row(label, str(count), status)
        
        add_row("Corrupted NIfTI files", stats['corrupted_nifti'], stats['total_nifti'])
        add_row("Corrupted JSON files", stats['corrupted_json'], stats['total_json'])
        add_row("Missing JSON sidecars", stats['missing_json'], stats['total_nifti'])
        add_row("Orphaned JSON files", stats['missing_nifti'], stats['total_json'])
        add_row("Suspicious file sizes", stats['suspicious_sizes'], stats['total_nifti'])
        
        console.print(table)
    
    # Show statistics
    console.print(f"\n[bold]Dataset Statistics:[/bold]")
    console.print(f"  Total NIfTI files: {stats['total_nifti']}")
    console.print(f"  Valid NIfTI files: {stats['valid_nifti']} ([green]{stats['valid_nifti']/stats['total_nifti']*100:.1f}%[/green])")
    console.print(f"  Total JSON files: {stats['total_json']}")
    console.print(f"  Valid JSON files: {stats['valid_json']} ([green]{stats['valid_json']/stats['total_json']*100:.1f}%[/green])" if stats['total_json'] > 0 else "  Valid JSON files: 0")
    
    # Detailed problem listing
    if verbose and total_problems > 0:
        console.print("\n" + "="*70)
        console.print("[bold]DETAILED PROBLEM LIST[/bold]")
        console.print("="*70)
        
        if problems['corrupted_nifti']:
            console.print("\n[bold red]Corrupted NIfTI Files:[/bold red]")
            for nii_path, error in problems['corrupted_nifti']:
                console.print(f"  [red]❌ {nii_path.relative_to(bids_dir)}[/red]")
                console.print(f"     Error: {error}")
        
        if problems['corrupted_json']:
            console.print("\n[bold red]Corrupted JSON Files:[/bold red]")
            for json_path, error in problems['corrupted_json']:
                console.print(f"  [red]❌ {json_path.relative_to(bids_dir)}[/red]")
                console.print(f"     Error: {error}")
        
        if problems['missing_json']:
            console.print("\n[bold yellow]Missing JSON Sidecars:[/bold yellow]")
            for nii_path in problems['missing_json']:
                console.print(f"  [yellow]⚠️  {nii_path.relative_to(bids_dir)}[/yellow]")
        
        if problems['missing_nifti']:
            console.print("\n[bold yellow]Orphaned JSON Files (no corresponding NIfTI):[/bold yellow]")
            for json_path in problems['missing_nifti']:
                console.print(f"  [yellow]⚠️  {json_path.relative_to(bids_dir)}[/yellow]")
        
        if problems['suspicious_sizes']:
            console.print("\n[bold yellow]Suspicious File Sizes (potential incomplete transfer):[/bold yellow]")
            for nii_path, size_mb, median_mb in problems['suspicious_sizes']:
                console.print(f"  [yellow]⚠️  {nii_path.relative_to(bids_dir)}[/yellow]")
                console.print(f"     Size: {size_mb:.1f} MB (median: {median_mb:.1f} MB)")
    
    # Save to file if requested
    if output:
        console.print(f"\n[cyan]Saving detailed report to {output}...[/cyan]")
        
        report = {
            'summary': stats,
            'problems': {
                'corrupted_nifti': [str(p[0]) for p in problems['corrupted_nifti']],
                'corrupted_json': [str(p[0]) for p in problems['corrupted_json']],
                'missing_json': [str(p) for p in problems['missing_json']],
                'missing_nifti': [str(p) for p in problems['missing_nifti']],
                'suspicious_sizes': [str(p[0]) for p in problems['suspicious_sizes']],
            }
        }
        
        with open(output, 'w') as f:
            json.dump(report, f, indent=2)
        
        console.print(f"[green]Report saved to {output}[/green]")
    
    # Return exit code based on problems found
    if stats['corrupted_nifti'] > 0 or stats['corrupted_json'] > 0:
        console.print("\n[bold red]⚠️  Critical problems found - some files are corrupted![/bold red]")
        return 1
    elif total_problems > 0:
        console.print("\n[bold yellow]⚠️  Some issues found - review the problems above[/bold yellow]")
        return 0
    else:
        return 0


@app.command()
def check_json_content(
    bids_dir: Path = typer.Argument(..., help="Path to BIDS dataset root or fMRIPrep derivatives"),
    show_fields: bool = typer.Option(
        False, "--show-fields", "-f",
        help="Show what fields are present in each JSON"
    ),
    n_jobs: int = typer.Option(
        15, "--n-jobs", "-j",
        help="Number of parallel jobs (default: 15)"
    ),
    is_fmriprep: bool = typer.Option(
        False, "--fmriprep",
        help="Check fMRIPrep derivatives directory"
    ),
    broken_files_list: Optional[Path] = typer.Option(
        None, "--broken-list", "-b",
        help="Save list of broken JSON file paths to text file"
    ),
):
    """
    Specifically check JSON files for empty or missing content.
    Useful when you have JSON files but they contain no data.
    Uses parallel processing for speed.
    """
    console.print(Panel.fit(
        f"[bold]JSON Content Validation[/bold]\n{bids_dir}\n[cyan]Using {n_jobs} parallel jobs[/cyan]",
        border_style="blue"
    ))
    
    files = find_bids_imaging_files(bids_dir, is_fmriprep=is_fmriprep)
    
    console.print(f"\n[cyan]Found {len(files['json'])} JSON files to check[/cyan]\n")
    
    problems = {
        'zero_byte': [],
        'empty_dict': [],
        'missing_fields': [],
        'valid': []
    }
    
    if files['json']:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            
            task = progress.add_task("Checking JSON content...", total=len(files['json']))
            
            # Parallel processing
            with Pool(processes=n_jobs) as pool:
                for json_path, is_valid, error, content in pool.imap_unordered(check_json_wrapper, files['json']):
                    if not is_valid:
                        if "Zero-byte" in error:
                            problems['zero_byte'].append((json_path, error))
                        elif "empty dictionary" in error:
                            problems['empty_dict'].append((json_path, error))
                        elif "Missing critical" in error:
                            problems['missing_fields'].append((json_path, error, content))
                        else:
                            problems['zero_byte'].append((json_path, error))
                    else:
                        problems['valid'].append((json_path, content))
                    
                    progress.update(task, advance=1)
    
    # Summary
    total_problems = len(problems['zero_byte']) + len(problems['empty_dict']) + len(problems['missing_fields'])
    
    console.print("\n" + "="*70)
    console.print("[bold]JSON CONTENT SUMMARY[/bold]")
    console.print("="*70 + "\n")
    
    table = Table(show_header=True, header_style="bold yellow")
    table.add_column("Issue Type", style="cyan")
    table.add_column("Count", justify="right", style="magenta")
    table.add_column("Status", justify="center")
    
    def add_row(label, count):
        if count == 0:
            status = "[green]✅[/green]"
        else:
            status = "[red]❌[/red]"
        table.add_row(label, str(count), status)
    
    add_row("Zero-byte files (completely empty)", len(problems['zero_byte']))
    add_row("Empty dictionary {} files", len(problems['empty_dict']))
    add_row("Missing critical BIDS fields", len(problems['missing_fields']))
    add_row("Valid JSON files", len(problems['valid']))
    
    console.print(table)
    
    # Detailed listing
    if problems['zero_byte']:
        console.print("\n[bold red]Zero-Byte JSON Files:[/bold red]")
        for json_path, error in problems['zero_byte']:
            console.print(f"  [red]❌ {json_path.relative_to(bids_dir)}[/red]")
            console.print(f"     {error}")
    
    if problems['empty_dict']:
        console.print("\n[bold red]Empty Dictionary {} Files:[/bold red]")
        for json_path, error in problems['empty_dict']:
            console.print(f"  [red]❌ {json_path.relative_to(bids_dir)}[/red]")
            console.print(f"     {error}")
    
    if problems['missing_fields']:
        console.print("\n[bold yellow]Missing Critical BIDS Fields:[/bold yellow]")
        for json_path, error, content in problems['missing_fields']:
            console.print(f"  [yellow]⚠️  {json_path.relative_to(bids_dir)}[/yellow]")
            console.print(f"     {error}")
            if show_fields and content:
                console.print(f"     Present fields: {', '.join(content.keys())}")
    
    # Show field statistics if requested
    if show_fields and problems['valid']:
        console.print("\n[bold cyan]Field Statistics (first 5 valid files):[/bold cyan]")
        for json_path, content in problems['valid'][:5]:
            console.print(f"\n  {json_path.relative_to(bids_dir)}")
            console.print(f"  Fields ({len(content)}): {', '.join(sorted(content.keys())[:10])}")
            if len(content) > 10:
                console.print(f"  ... and {len(content) - 10} more fields")
    
    # Save broken files list to text file
    if broken_files_list and total_problems > 0:
        console.print(f"\n[cyan]Saving broken JSON files list to {broken_files_list}...[/cyan]")
        
        with open(broken_files_list, 'w') as f:
            # Write header
            f.write(f"# JSON Content Check - Broken Files\n")
            f.write(f"# Dataset: {bids_dir}\n")
            f.write(f"# Total problems: {total_problems}\n")
            f.write(f"# Generated: {Path.cwd()}\n\n")
            
            # Write zero-byte files
            if problems['zero_byte']:
                f.write("# Zero-Byte JSON Files\n")
                for json_path, error in problems['zero_byte']:
                    f.write(f"{json_path}\n")
                f.write("\n")
            
            # Write empty dictionary files
            if problems['empty_dict']:
                f.write("# Empty Dictionary {} Files\n")
                for json_path, error in problems['empty_dict']:
                    f.write(f"{json_path}\n")
                f.write("\n")
            
            # Write missing fields files
            if problems['missing_fields']:
                f.write("# Files Missing Critical BIDS Fields\n")
                for json_path, error, content in problems['missing_fields']:
                    f.write(f"{json_path}\n")
                f.write("\n")
        
        console.print(f"[green]Broken JSON files list saved to {broken_files_list}[/green]")
        console.print(f"[green]Total broken files: {total_problems}[/green]")
    
    # Final message
    console.print("\n" + "="*70)
    if total_problems == 0:
        console.print("[bold green]✅ All JSON files have content![/bold green]")
    else:
        console.print(f"[bold red]❌ Found {total_problems} JSON files with content issues[/bold red]")
        console.print("\n[yellow]These files exist but are empty or incomplete.[/yellow]")
        console.print("[yellow]You may need to regenerate them from the original DICOM files.[/yellow]")
    
    return 0 if total_problems == 0 else 1


@app.command()
def quick_summary(
    bids_dir: Path = typer.Argument(..., help="Path to BIDS dataset root or fMRIPrep derivatives"),
    is_fmriprep: bool = typer.Option(
        False, "--fmriprep",
        help="Check fMRIPrep derivatives directory"
    ),
):
    """
    Quick check - just count files and report basic statistics.
    Works for both raw BIDS data and fMRIPrep derivatives.
    """
    console.print(Panel.fit(
        f"[bold]Quick BIDS Summary[/bold]\n{bids_dir}",
        border_style="blue"
    ))
    
    files = find_bids_imaging_files(bids_dir, is_fmriprep=is_fmriprep)
    nifti_to_json, json_to_nifti, orphaned = get_nifti_json_pairs(bids_dir, is_fmriprep=is_fmriprep)
    
    # Count by subject and session
    by_subject = defaultdict(lambda: defaultdict(lambda: {'nifti': 0, 'json': 0}))
    
    for nii_path in files['nifti']:
        parts = nii_path.parts
        for i, part in enumerate(parts):
            if part.startswith('sub-'):
                subject = part
                session = parts[i+1] if i+1 < len(parts) and parts[i+1].startswith('ses-') else 'no_session'
                by_subject[subject][session]['nifti'] += 1
                break
    
    for json_path in files['json']:
        parts = json_path.parts
        for i, part in enumerate(parts):
            if part.startswith('sub-'):
                subject = part
                session = parts[i+1] if i+1 < len(parts) and parts[i+1].startswith('ses-') else 'no_session'
                by_subject[subject][session]['json'] += 1
                break
    
    # Create summary table
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Subject", style="cyan")
    table.add_column("Session", style="yellow")
    table.add_column("NIfTI", justify="right")
    table.add_column("JSON", justify="right")
    table.add_column("Status", justify="center")
    
    for subject in sorted(by_subject.keys()):
        for session in sorted(by_subject[subject].keys()):
            counts = by_subject[subject][session]
            nifti_count = counts['nifti']
            json_count = counts['json']
            
            if nifti_count == json_count:
                status = "[green]✅[/green]"
            else:
                status = "[red]⚠️[/red]"
            
            table.add_row(subject, session, str(nifti_count), str(json_count), status)
    
    console.print("\n")
    console.print(table)
    
    console.print(f"\n[bold]Total Files:[/bold]")
    console.print(f"  NIfTI: {len(files['nifti'])}")
    console.print(f"  JSON: {len(files['json'])}")
    console.print(f"  Paired: {len(nifti_to_json)}")
    console.print(f"  Orphaned: {len(orphaned)}")
    
    if len(orphaned) > 0:
        console.print(f"\n[yellow]⚠️  {len(orphaned)} files are missing their pair[/yellow]")


if __name__ == "__main__":
    app()