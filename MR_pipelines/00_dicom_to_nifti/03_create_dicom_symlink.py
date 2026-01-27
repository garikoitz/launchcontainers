#!/usr/bin/env python3
"""
Create BIDS-organized symlinks from lab DICOM directories.

This script reads a CSV mapping file and creates symlinks from the original
lab DICOM directories to a BIDS-structured directory for heudiconv processing.
"""

import os
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Set, List, Tuple
import typer
from rich.console import Console
from rich.progress import track
from rich.table import Table

app = typer.Typer()
console = Console()


def create_dicom_symlink(
    source_dir: Path,
    target_dir: Path,
    sub_id: str,
    ses_id: str,
    suffix: str,
    levels_from_top: int,
    dry_run: bool = False,
    force: bool = False,
) -> bool:
    """
    Create a symlink from source DICOM directory to BIDS-organized target.
    
    Parameters
    ----------
    source_dir : Path
        Source directory containing DICOM files
    target_dir : Path
        Base target directory for BIDS structure
    sub_id : str
        Subject ID (e.g., '01')
    ses_id : str
        Session ID (e.g., '01', '02')
    suffix : str
        Session suffix (e.g., 'part1', 'part2', or empty string)
    levels_from_top : int
        Number of directory levels from source_dir to DICOM files
    dry_run : bool
        If True, only print what would be done without creating links
    force : bool
        If True, overwrite existing symlinks/directories
        
    Returns
    -------
    bool
        True if successful, False otherwise
    """
    # Construct BIDS-style target path with suffix
    bids_sub = f"sub-{sub_id}"
    full_ses_id = f"{ses_id}{suffix}" if suffix else ses_id
    bids_ses = f"ses-{full_ses_id}"
    target_path = target_dir / bids_sub / bids_ses
    
    # Check if source exists
    if not source_dir.exists():
        console.print(f"[red]✗[/red] Source not found: {source_dir}")
        return False
    
    # Check if target already exists
    if target_path.exists():
        if target_path.is_symlink():
            existing_target = target_path.resolve()
            if existing_target == source_dir.resolve():
                console.print(f"[dim]○ Already linked: {bids_sub}/{bids_ses}[/dim]")
                return True
            else:
                if force:
                    if dry_run:
                        console.print(
                            f"[blue]DRY RUN[/blue] Would overwrite: {bids_sub}/{bids_ses} "
                            f"(currently → {existing_target})"
                        )
                        return True
                    else:
                        console.print(
                            f"[yellow]⚠[/yellow] Overwriting existing link: {bids_sub}/{bids_ses}"
                        )
                        target_path.unlink()
                else:
                    console.print(
                        f"[yellow]⚠[/yellow] Link exists but points elsewhere: "
                        f"{bids_sub}/{bids_ses} → {existing_target}"
                    )
                    console.print(
                        f"[dim]    Use --force to overwrite[/dim]"
                    )
                    return False
        else:
            # Target exists but is not a symlink (it's a directory or file)
            if force:
                if dry_run:
                    console.print(
                        f"[blue]DRY RUN[/blue] Would remove and replace: {bids_sub}/{bids_ses} "
                        f"(currently a {'directory' if target_path.is_dir() else 'file'})"
                    )
                    return True
                else:
                    console.print(
                        f"[yellow]⚠[/yellow] Removing existing {'directory' if target_path.is_dir() else 'file'}: "
                        f"{bids_sub}/{bids_ses}"
                    )
                    if target_path.is_dir():
                        import shutil
                        shutil.rmtree(target_path)
                    else:
                        target_path.unlink()
            else:
                console.print(
                    f"[red]✗[/red] Target exists and is not a symlink: {target_path}"
                )
                console.print(
                    f"[dim]    Use --force to overwrite[/dim]"
                )
                return False
    
    if dry_run:
        console.print(
            f"[blue]DRY RUN[/blue] Would create: {bids_sub}/{bids_ses} → {source_dir}"
        )
        return True
    
    # Create parent directories if needed
    target_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create symlink
    try:
        target_path.symlink_to(source_dir, target_is_directory=True)
        console.print(f"[green]✓[/green] Created: {bids_sub}/{bids_ses}")
        return True
    except Exception as e:
        console.print(
            f"[red]✗[/red] Failed to create {bids_sub}/{bids_ses}: {e}"
        )
        return False


def validate_row(row: pd.Series) -> Optional[str]:
    """
    Validate a CSV row has required fields and correct quality flags.
    
    Parameters
    ----------
    row : pd.Series
        Row from the mapping CSV
        
    Returns
    -------
    str or None
        Error message if validation fails, None if valid
    """
    # Check for required columns
    required = ['sub', 'ses_id_from_xlsx', 'ses_from_dcm', 'lab_project_dir', 'dir_name', 'levels_from_top']
    for col in required:
        if pd.isna(row.get(col)):
            return f"Missing required field: {col}"
    
    # Check 3: session_correct must be 1 (or not 0)
    if row.get('session_correct') == 0:
        return "session_correct=0"
    
    # Check quality flags
    if row.get('note_from_dcm') == 'wrong' or row.get('note_from_note') == 'wrong':
        return "Flagged as 'wrong' in quality notes"
    
    return None


def resolve_session_conflicts(group_df: pd.DataFrame) -> List[Tuple[pd.Series, str, str]]:
    """
    Resolve conflicts within a (sub, ses_id_from_xlsx) group.
    
    Logic:
    1. If both 'base' and 'manual' exist with same ses_from_dcm+suffix → prefer 'base'
    2. After filtering, if only one unique row remains → use ses_id_from_xlsx, ignore suffix
    3. If multiple different rows remain → use ses_from_dcm + suffix for each
    
    Parameters
    ----------
    group_df : pd.DataFrame
        Rows with the same (sub, ses_id_from_xlsx)
        
    Returns
    -------
    list of tuples
        (row, final_ses_id, final_suffix) for each row to process
    """
    # Build a map of (ses_from_dcm, suffix) → origins
    session_variants = {}
    
    for idx, row in group_df.iterrows():
        ses_from_dcm = str(row['ses_from_dcm'])
        suffix = row.get('suffix_from_dcm', '') or ''
        if pd.isna(suffix):
            suffix = ''
        else:
            suffix = str(suffix).strip()
        
        origin = row['origin']
        key = (ses_from_dcm, suffix)
        
        if key not in session_variants:
            session_variants[key] = []
        session_variants[key].append((idx, row, origin))
    
    # For each variant, prefer 'base' over 'manual'
    selected_rows = []
    
    for (ses_from_dcm, suffix), variants in session_variants.items():
        # Check if we have both base and manual
        has_base = any(origin == 'base' for _, _, origin in variants)
        has_manual = any(origin == 'manual' for _, _, origin in variants)
        
        if has_base and has_manual:
            # Prefer base, skip manual
            for idx, row, origin in variants:
                if origin == 'base':
                    selected_rows.append((row, ses_from_dcm, suffix))
                else:
                    console.print(
                        f"[dim]⊘ Preferring base over manual for sub-{row['sub']} "
                        f"ses_xlsx-{row['ses_id_from_xlsx']} "
                        f"(ses_dcm-{ses_from_dcm}{suffix})[/dim]"
                    )
        else:
            # No conflict, keep all
            for idx, row, origin in variants:
                selected_rows.append((row, ses_from_dcm, suffix))
    
    # Determine final session IDs
    ses_id_from_xlsx = str(group_df.iloc[0]['ses_id_from_xlsx']).zfill(2)
    
    result = []
    
    if len(selected_rows) == 1:
        # Single row: use ses_id_from_xlsx, ignore suffix
        row, ses_from_dcm, suffix = selected_rows[0]
        result.append((row, ses_id_from_xlsx, ''))  # Empty suffix!
    else:
        # Multiple rows: use ses_from_dcm + suffix to distinguish
        for row, ses_from_dcm, suffix in selected_rows:
            final_ses_id = str(ses_from_dcm).zfill(2)
            result.append((row, final_ses_id, suffix))
    
    return result

@app.command()
def create_symlinks(
    mapping_csv: Path = typer.Argument(
        ...,
        help="Path to dcm_bids_mapping_summary.csv",
        exists=True,
    ),
    target_base: Path = typer.Argument(
        ...,
        help="Base directory for BIDS symlinks (e.g., /basedir/dicom)",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        "-n",
        help="Show what would be done without creating symlinks",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Overwrite existing symlinks and directories",
    ),
    skip_manual: bool = typer.Option(
        False,
        "--skip-manual",
        help="Skip manual DICOM directories (origin='manual')",
    ),
    skip_base: bool = typer.Option(
        False,
        "--skip-base",
        help="Skip base DICOM directories (origin='base')",
    ),
    force_origin: Optional[str] = typer.Option(
        None,
        "--origin",
        help="Only process specific origin ('base' or 'manual')",
    ),
):
    """
    Create BIDS-organized symlinks from lab DICOM directories.
    
    Session ID logic:
    1. Groups by (sub, ses_id_from_xlsx)
    2. Within each group, if both base and manual exist with same ses_from_dcm+suffix → prefer base
    3. If single row in group → use ses_id_from_xlsx as session ID
    4. If multiple rows in group → use ses_from_dcm to distinguish them
    5. Skip sessions where session_correct=0 and report them
    
    Session ID construction:
    - Single row: ses_id_from_xlsx + suffix_from_dcm
    - Multiple rows: ses_from_dcm + suffix_from_dcm
    
    Examples
    --------
    # Dry run to preview
    python create_symlinks.py mapping.csv /basedir/dicom --dry-run
    
    # Create symlinks, overwriting any existing ones
    python create_symlinks.py mapping.csv /basedir/dicom --force
    
    # Create only base (not manual) symlinks
    python create_symlinks.py mapping.csv /basedir/dicom --skip-manual
    """
    console.print(f"\n[bold]Reading mapping CSV:[/bold] {mapping_csv}")
    
    # Read CSV
    try:
        df = pd.read_csv(mapping_csv)
    except Exception as e:
        console.print(f"[red]Error reading CSV:[/red] {e}")
        raise typer.Exit(1)
    
    console.print(f"Found {len(df)} rows in mapping file")
    
    # Check 3: Find and report sessions with session_correct=0
    invalid_sessions = df[df['session_correct'] == 0]
    if len(invalid_sessions) > 0:
        console.print(f"\n[yellow]⚠ Found {len(invalid_sessions)} invalid sessions (session_correct=0):[/yellow]")
        
        table = Table(title="Invalid Sessions (Will NOT Create Symlinks)")
        table.add_column("Subject", style="cyan")
        table.add_column("XLSX Ses", style="blue")
        table.add_column("DCM Ses", style="magenta")
        table.add_column("Suffix", style="yellow")
        table.add_column("Origin", style="blue")
        table.add_column("Directory", style="dim")
        
        for _, row in invalid_sessions.iterrows():
            sub_id = f"sub-{str(row['sub']).zfill(2)}"
            ses_xlsx = f"{row['ses_id_from_xlsx']}"
            ses_dcm = f"{row['ses_from_dcm']}"
            suffix = row.get('suffix_from_dcm', '') or ''
            
            table.add_row(
                sub_id,
                ses_xlsx,
                ses_dcm,
                suffix,
                row.get('origin', ''),
                str(Path(row['lab_project_dir']) / row['dir_name'])
            )
        
        console.print(table)
        console.print()
    
    # Filter out invalid sessions
    df = df[df['session_correct'] != 0].copy()
    
    # Apply origin filters before grouping
    if force_origin:
        df = df[df['origin'] == force_origin]
        console.print(f"Filtering to origin='{force_origin}': {len(df)} rows")
    else:
        if skip_manual:
            df = df[df['origin'] != 'manual']
            console.print(f"Skipping manual: {len(df)} rows remaining")
        if skip_base:
            df = df[df['origin'] != 'base']
            console.print(f"Skipping base: {len(df)} rows remaining")
    
    if len(df) == 0:
        console.print("[yellow]No rows to process after filtering[/yellow]")
        raise typer.Exit(0)
    
    console.print(f"\n[bold]Target base directory:[/bold] {target_base}")
    if dry_run:
        console.print("[blue]DRY RUN MODE - No changes will be made[/blue]")
    if force:
        console.print("[yellow]FORCE MODE - Will overwrite existing symlinks/directories[/yellow]")
    console.print()
    
    # Group by (sub, ses_id_from_xlsx) and resolve conflicts
    console.print("[cyan]Resolving session conflicts...[/cyan]")
    
    rows_to_process = []
    skipped_prefer_base = 0
    
    for (sub, ses_xlsx), group_df in df.groupby(['sub', 'ses_id_from_xlsx']):
        # Validate all rows in group
        valid_group = []
        for idx, row in group_df.iterrows():
            error = validate_row(row)
            if error is None:
                valid_group.append(row)
        
        if not valid_group:
            continue
        
        # Convert back to DataFrame for resolve function
        valid_group_df = pd.DataFrame(valid_group)
        
        # Count before resolution
        before_count = len(valid_group_df)
        
        # Resolve conflicts within this group
        selected = resolve_session_conflicts(valid_group_df)
        
        # Count skipped
        skipped_prefer_base += (before_count - len(selected))
        
        rows_to_process.extend(selected)
    
    console.print(f"After conflict resolution: {len(rows_to_process)} sessions to process")
    console.print()
    
    # Process each selected row
    results = {
        'success': 0,
        'skipped': 0,
        'failed': 0,
        'already_exists': 0,
        'skipped_prefer_base': skipped_prefer_base,
        'overwritten': 0,
    }
    
    for row, final_ses_id, final_suffix in track(
        rows_to_process,
        total=len(rows_to_process),
        description="Creating symlinks...",
    ):
        sub_id = str(row['sub']).zfill(2)
        
        # Construct source directory
        lab_dir = Path(row['lab_project_dir'])
        source_dir = lab_dir / row['dir_name']
        
        # Track if we're overwriting
        bids_sub = f"sub-{sub_id}"
        full_ses_id = f"{final_ses_id}{final_suffix}" if final_suffix else final_ses_id
        bids_ses = f"ses-{full_ses_id}"
        target_path = target_base / bids_sub / bids_ses
        will_overwrite = target_path.exists() and force
        
        # Create symlink
        success = create_dicom_symlink(
            source_dir=source_dir,
            target_dir=target_base,
            sub_id=sub_id,
            ses_id=final_ses_id,
            suffix=final_suffix,
            levels_from_top=int(row['levels_from_top']),
            dry_run=dry_run,
            force=force,
        )
        
        if success:
            if will_overwrite:
                results['overwritten'] += 1
            results['success'] += 1
        else:
            results['failed'] += 1
    
    # Print summary
    console.print("\n[bold]Summary:[/bold]")
    console.print(f"  [green]✓ Created:[/green] {results['success']}")
    if results['overwritten'] > 0:
        console.print(f"  [yellow]↻ Overwritten:[/yellow] {results['overwritten']}")
    console.print(f"  [yellow]⊘ Skipped (quality):[/yellow] {results['skipped']}")
    console.print(f"  [cyan]⊘ Skipped (prefer base):[/cyan] {results['skipped_prefer_base']}")
    console.print(f"  [red]✗ Failed:[/red] {results['failed']}")
    
    if len(invalid_sessions) > 0:
        console.print(f"  [yellow]⊘ Invalid (session_correct=0):[/yellow] {len(invalid_sessions)}")
    
    if results['failed'] > 0:
        console.print("\n[yellow]Some symlinks failed to create. Check errors above.[/yellow]")
        raise typer.Exit(1)
    
    if dry_run:
        console.print("\n[blue]Dry run complete. Use without --dry-run to create symlinks.[/blue]")


if __name__ == '__main__':
    app()