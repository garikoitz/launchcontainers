#!/usr/bin/env python3
"""
DICOM Symlink Organizer
-----------------------
Reorganizes DICOM data by creating symlinks with a standardized structure.

Part of the MRIworkflow project.

Create from src: dcm folder

to targ: project/dicom folder with structure: sub-xx/ses-yy
"""

import os
import re
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import typer
from rich.console import Console
from rich.table import Table
from rich.progress import track

app = typer.Typer(
    name="dicom-organizer",
    help="Reorganize DICOM data by creating symlinks with standardized structure",
    add_completion=False,
)

console = Console()





def count_layers_to_dicom(session_path: Path) -> int:
    """
    Count the number of directory layers from session folder to first DICOM file.
    
    Args:
        session_path: Path to the session folder
        
    Returns:
        Number of layers, or -1 if no DICOM files found
    """
    min_layers = float('inf')
    
    for root, dirs, files in os.walk(session_path):
        for file in files:
            if file.lower().endswith('.dcm'):
                rel_path = os.path.relpath(os.path.join(root, file), session_path)
                layers = len(Path(rel_path).parts) - 1
                min_layers = min(min_layers, layers)
    
    return int(min_layers) if min_layers != float('inf') else -1


def find_all_dicom_files(session_path: Path) -> List[Tuple[str, str]]:
    """
    Find all DICOM files under a session folder.
    
    Args:
        session_path: Path to the session folder
        
    Returns:
        List of tuples (relative_path_from_session, full_path)
    """
    dicom_files = []
    
    for root, dirs, files in os.walk(session_path):
        for file in files:
            if file.lower().endswith('.dcm'):
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, session_path)
                dicom_files.append((rel_path, full_path))
    
    return dicom_files


def create_symlink_structure(
    source_base: Path,
    target_base: Path,
    dry_run: bool = False,
    verbose: bool = True
) -> List[Dict]:
    """
    Create symlink structure from source to target.
    
    Args:
        source_base: Base path of source data
        target_base: Base path for symlinks
        dry_run: If True, only print what would be done without creating links
        verbose: If True, print detailed progress
        
    Returns:
        List of dictionaries containing session information
        
    Raises:
        FileNotFoundError: If source path does not exist
    """
    if not source_base.exists():
        raise FileNotFoundError(f"Source path does not exist: {source_base}")
    
    session_info = []
    session_folders = [f for f in source_base.iterdir() if f.is_dir()]
    
    folder_iter = track(session_folders, description="Processing sessions...") if verbose else session_folders
    
    for session_folder in folder_iter:
        sub_id, ses_id = _parse_session_folder_name(session_folder.name)
        
        if sub_id is None or ses_id is None:
            console.print(f"[yellow]Warning: Could not parse folder name: {session_folder.name}[/yellow]")
            continue
        
        num_layers = count_layers_to_dicom(session_folder)
        
        if num_layers == -1:
            console.print(f"[yellow]Warning: No DICOM files found in {session_folder.name}[/yellow]")
            continue
        
        if verbose:
            console.print(f"[green]✓[/green] {session_folder.name} → sub-{sub_id}, ses-{ses_id}, layers={num_layers}")
        
        session_info.append({
            'sub': sub_id,
            'ses': ses_id,
            'layer': num_layers,
            'source_folder': session_folder.name
        })
        
        dicom_files = find_all_dicom_files(session_folder)
        
        if verbose:
            console.print(f"  Found {len(dicom_files)} DICOM files")
        
        for rel_path, full_path in dicom_files:
            target_file = target_base / f"sub-{sub_id}" / f"ses-{ses_id}" / rel_path
            
            if not dry_run:
                target_file.parent.mkdir(parents=True, exist_ok=True)
                
                if target_file.exists() or target_file.is_symlink():
                    target_file.unlink()
                
                target_file.symlink_to(full_path)
            else:
                if verbose:
                    console.print(f"  Would create: {target_file} → {full_path}")
        
        if not dry_run and verbose:
            console.print(f"  [green]Created {len(dicom_files)} symlinks[/green]")
    
    return session_info


def write_summary(session_info: List[Dict], output_file: Path) -> None:
    """
    Write summary file with session information.
    
    Args:
        session_info: List of session information dictionaries
        output_file: Path to output file
    """
    session_info.sort(key=lambda x: (x['sub'], x['ses']))
    
    with open(output_file, 'w') as f:
        f.write("sub\tses\tlayer\n")
        
        for info in session_info:
            f.write(f"{info['sub']}\t{info['ses']}\t{info['layer']}\n")
    
    console.print(f"\n[green]✓ Summary written to:[/green] {output_file}")
    console.print(f"[green]✓ Total sessions processed:[/green] {len(session_info)}")


def display_summary_table(session_info: List[Dict]) -> None:
    """Display summary in a rich table format."""
    if not session_info:
        console.print("[yellow]No sessions to display[/yellow]")
        return
    
    table = Table(title="Session Summary", show_header=True, header_style="bold magenta")
    table.add_column("Subject", style="cyan", justify="right")
    table.add_column("Session", style="cyan", justify="right")
    table.add_column("Layers", style="green", justify="right")
    table.add_column("Source Folder", style="dim")
    
    sorted_info = sorted(session_info, key=lambda x: (x['sub'], x['ses']))
    
    for info in sorted_info:
        table.add_row(
            f"sub-{info['sub']}",
            f"ses-{info['ses']}",
            str(info['layer']),
            info['source_folder']
        )
    
    console.print(table)


@app.command()
def organize(
    source: Path = typer.Option(
        ...,
        "--source",
        "-s",
        help="Source directory containing DICOM data",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
    ),
    target: Path = typer.Option(
        ...,
        "--target",
        "-t",
        help="Target directory for symlinks",
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
    ),
    output: Path = typer.Option(
        "subseslist.txt",
        "--output",
        "-o",
        help="Output summary file",
        dir_okay=False,
        resolve_path=True,
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        "-d",
        help="Preview operations without creating symlinks",
    ),
    quiet: bool = typer.Option(
        False,
        "--quiet",
        "-q",
        help="Suppress detailed output",
    ),
):
    """
    Reorganize DICOM data by creating symlinks with standardized structure.
    
    Supported session naming formats:
    - sub-01_ses-01 (standard)
    - S1T1 or S01T01 (compact)
    - VOTCLOC_S1T1 (with prefix)
    """
    verbose = not quiet
    
    if verbose:
        console.print("\n[bold cyan]DICOM Symlink Organizer[/bold cyan]")
        console.print("=" * 70)
        console.print(f"[bold]Source:[/bold] {source}")
        console.print(f"[bold]Target:[/bold] {target}")
        console.print(f"[bold]Output:[/bold] {output}")
        console.print(f"[bold]Mode:[/bold] {'[yellow]DRY RUN[/yellow]' if dry_run else '[green]LIVE[/green]'}")
        console.print("=" * 70 + "\n")
    
    try:
        session_info = create_symlink_structure(
            source,
            target,
            dry_run=dry_run,
            verbose=verbose
        )
        
        if not session_info:
            console.print("[yellow]No valid sessions found to process[/yellow]")
            raise typer.Exit(code=1)
        
        if verbose:
            console.print()
            display_summary_table(session_info)
        
        if not dry_run:
            write_summary(session_info, output)
        else:
            if verbose:
                console.print("\n[yellow]Dry run complete. Run without --dry-run to create symlinks.[/yellow]")
        
    except FileNotFoundError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[bold red]Unexpected error:[/bold red] {e}")
        if verbose:
            console.print_exception()
        raise typer.Exit(code=1)


@app.command()
def version():
    """Show version information."""
    console.print("[bold cyan]DICOM Symlink Organizer[/bold cyan]")
    console.print("Version: 1.0.0")
    console.print("Part of MRIworkflow project")


if __name__ == "__main__":
    app()