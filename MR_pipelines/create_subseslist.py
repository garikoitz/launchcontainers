#!/usr/bin/env python3
"""
Filter subseslist based on modality and depth criteria.

This script reads a CSV file with session data and creates a filtered subseslist.txt
based on user-specified criteria.
"""
from pathlib import Path
from typing import Optional, List
import pandas as pd

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer()
console = Console()


def read_session_data(input_file: Path) -> pd.DataFrame:
    """Read session data from CSV file."""
    df = pd.read_csv(input_file)
    
    # Convert boolean strings to actual booleans
    bool_columns = ['DWI', 'func_floc', 'func_prf', 't1', 't2']
    for col in bool_columns:
        df[col] = df[col].map(lambda x: str(x).lower() in ('true', '1', 'yes', 'y'))
    
    # Convert levels to integer, handling empty values
    df['levels'] = pd.to_numeric(df['levels'], errors='coerce').fillna(0).astype(int)
    df.loc[df['levels'] == 0, 'levels'] = pd.NA
    
    # Ensure sub and ses are strings
    df['sub'] = df['sub'].astype(str).str.zfill(2)
    df['ses'] = df['ses'].astype(str).str.zfill(2)
    
    return df


def filter_sessions(
    df: pd.DataFrame,
    require_dwi: Optional[bool] = None,
    require_floc: Optional[bool] = None,
    require_prf: Optional[bool] = None,
    require_t1: Optional[bool] = None,
    require_t2: Optional[bool] = None,
    require_levels: Optional[int] = None,
    exclude_dwi: Optional[bool] = None,
    exclude_floc: Optional[bool] = None,
    exclude_prf: Optional[bool] = None,
    exclude_t1: Optional[bool] = None,
    exclude_t2: Optional[bool] = None,
    exclude_levels: Optional[int] = None,
    subjects: Optional[List[str]] = None,
    exclude_subjects: Optional[List[str]] = None,
    sessions_range: Optional[List[str]] = None,
    exclude_sessions: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Filter sessions based on criteria.
    
    Args:
        df: DataFrame with session data
        require_*: If True, only include sessions with this modality
        exclude_*: If True, exclude sessions with this modality
        require_levels: Only include sessions with this depth level
        exclude_levels: Exclude sessions with this depth level
        subjects: List of specific subjects to include (e.g., ['01', '02'])
        exclude_subjects: List of subjects to exclude
        sessions_range: List of specific sessions to include (e.g., ['01', '02'])
        exclude_sessions: List of sessions to exclude
    
    Returns:
        Filtered DataFrame
    """
    filtered = df.copy()
    
    # Check inclusion criteria (require_*)
    if require_dwi is not None:
        filtered = filtered[filtered['DWI'] == require_dwi]
    if require_floc is not None:
        filtered = filtered[filtered['func_floc'] == require_floc]
    if require_prf is not None:
        filtered = filtered[filtered['func_prf'] == require_prf]
    if require_t1 is not None:
        filtered = filtered[filtered['t1'] == require_t1]
    if require_t2 is not None:
        filtered = filtered[filtered['t2'] == require_t2]
    if require_levels is not None:
        filtered = filtered[filtered['levels'] == require_levels]
    
    # Check exclusion criteria (exclude_*)
    if exclude_dwi is not None:
        filtered = filtered[filtered['DWI'] != exclude_dwi]
    if exclude_floc is not None:
        filtered = filtered[filtered['func_floc'] != exclude_floc]
    if exclude_prf is not None:
        filtered = filtered[filtered['func_prf'] != exclude_prf]
    if exclude_t1 is not None:
        filtered = filtered[filtered['t1'] != exclude_t1]
    if exclude_t2 is not None:
        filtered = filtered[filtered['t2'] != exclude_t2]
    if exclude_levels is not None:
        filtered = filtered[filtered['levels'] != exclude_levels]
    
    # Check subject filters
    if subjects is not None:
        filtered = filtered[filtered['sub'].isin(subjects)]
    if exclude_subjects is not None:
        filtered = filtered[~filtered['sub'].isin(exclude_subjects)]
    
    # Check session filters
    if sessions_range is not None:
        filtered = filtered[filtered['ses'].isin(sessions_range)]
    if exclude_sessions is not None:
        filtered = filtered[~filtered['ses'].isin(exclude_sessions)]
    
    return filtered


def write_subseslist(df: pd.DataFrame, output_file: Path):
    """Write filtered sessions to subseslist.txt format with comma separation."""
    output_df = df[['sub', 'ses']].copy()
    output_df = output_df.sort_values(['sub', 'ses'])
    
    # Write with comma separator - NO prefixes, just IDs
    output_df.to_csv(output_file, sep=',', index=False)


def print_summary(original_count: int, filtered_count: int, df: pd.DataFrame):
    """Print summary of filtering results."""
    console.print(f"\n[bold]Filtering Summary:[/bold]")
    console.print(f"  Original sessions: {original_count}")
    console.print(f"  Filtered sessions: {filtered_count}")
    console.print(f"  Removed: {original_count - filtered_count}")
    
    if filtered_count > 0:
        # Count modalities in filtered sessions
        dwi_count = df['DWI'].sum()
        floc_count = df['func_floc'].sum()
        prf_count = df['func_prf'].sum()
        t1_count = df['t1'].sum()
        t2_count = df['t2'].sum()
        
        console.print(f"\n[bold]Filtered Session Modalities:[/bold]")
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Modality", style="cyan")
        table.add_column("Count", style="green")
        table.add_column("Percentage", style="blue")
        
        modalities = [
            ("DWI", dwi_count),
            ("fLoc", floc_count),
            ("PRF", prf_count),
            ("T1", t1_count),
            ("T2", t2_count),
        ]
        
        for mod_name, count in modalities:
            pct = f"{100*count/filtered_count:.1f}%"
            table.add_row(mod_name, str(count), pct)
        
        console.print(table)
        
        # Show subject distribution
        subject_counts = df['sub'].value_counts().sort_index()
        console.print(f"\n[bold]Sessions per Subject:[/bold]")
        for sub, count in subject_counts.items():
            console.print(f"  sub-{sub}: {count} sessions")


@app.command()
def main(
    input_file: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Input CSV file (e.g., subseslist_dcm.txt)"
    ),
    output_file: Path = typer.Argument(
        ...,
        help="Output subseslist.txt file (comma-separated)"
    ),
    # Require modalities (include only if True)
    require_dwi: bool = typer.Option(
        False,
        "--require-dwi",
        help="Only include sessions with complete DWI"
    ),
    require_floc: bool = typer.Option(
        False,
        "--require-floc",
        help="Only include sessions with complete fLoc"
    ),
    require_prf: bool = typer.Option(
        False,
        "--require-prf",
        help="Only include sessions with complete PRF/retinotopy"
    ),
    require_t1: bool = typer.Option(
        False,
        "--require-t1",
        help="Only include sessions with complete T1"
    ),
    require_t2: bool = typer.Option(
        False,
        "--require-t2",
        help="Only include sessions with complete T2"
    ),
    require_all: bool = typer.Option(
        False,
        "--require-all",
        help="Require all modalities (DWI, fLoc, PRF, T1, T2)"
    ),
    # Exclude modalities (exclude if True)
    exclude_dwi: bool = typer.Option(
        False,
        "--exclude-dwi",
        help="Exclude sessions with DWI"
    ),
    exclude_floc: bool = typer.Option(
        False,
        "--exclude-floc",
        help="Exclude sessions with fLoc"
    ),
    exclude_prf: bool = typer.Option(
        False,
        "--exclude-prf",
        help="Exclude sessions with PRF"
    ),
    exclude_t1: bool = typer.Option(
        False,
        "--exclude-t1",
        help="Exclude sessions with T1"
    ),
    exclude_t2: bool = typer.Option(
        False,
        "--exclude-t2",
        help="Exclude sessions with T2"
    ),
    # Depth level filters
    require_levels: Optional[int] = typer.Option(
        None,
        "--require-levels",
        "-l",
        help="Only include sessions with this depth level (e.g., 1 or 3)"
    ),
    exclude_levels: Optional[int] = typer.Option(
        None,
        "--exclude-levels",
        help="Exclude sessions with this depth level"
    ),
    # Subject filters
    subjects: Optional[str] = typer.Option(
        None,
        "--subjects",
        "-s",
        help="Comma-separated list of subjects to include (e.g., '01,02,03')"
    ),
    exclude_subjects: Optional[str] = typer.Option(
        None,
        "--exclude-subjects",
        help="Comma-separated list of subjects to exclude"
    ),
    # Session filters
    sessions: Optional[str] = typer.Option(
        None,
        "--sessions",
        help="Comma-separated list of sessions to include (e.g., '01,02,03')"
    ),
    exclude_sessions: Optional[str] = typer.Option(
        None,
        "--exclude-sessions",
        help="Comma-separated list of sessions to exclude"
    ),
    # Display options
    show_filtered: bool = typer.Option(
        False,
        "--show-filtered",
        "-v",
        help="Show detailed list of filtered sessions"
    ),
):
    """
    Filter subseslist based on modality and other criteria.
    
    This script reads a CSV file with session data (output from check_dicom_counts.py)
    and creates a filtered subseslist.txt based on your criteria.
    
    Examples:
        # Get all sessions with complete DWI and fLoc
        filter_subseslist.py input.csv output.txt --require-dwi --require-floc
        
        # Get all sessions with all modalities complete
        filter_subseslist.py input.csv output.txt --require-all
        
        # Get sessions from specific subjects
        filter_subseslist.py input.csv output.txt --subjects 01,02,03
        
        # Exclude subjects 02 and 03
        filter_subseslist.py input.csv output.txt --exclude-subjects 02,03
        
        # Get only sessions with depth level 1
        filter_subseslist.py input.csv output.txt --require-levels 1
        
        # Get sessions 01-05 only
        filter_subseslist.py input.csv output.txt --sessions 01,02,03,04,05
        
        # Complex: All modalities + specific subjects + exclude session 01
        filter_subseslist.py input.csv output.txt --require-all --subjects 01,02,03 --exclude-sessions 01
    """
    console.print(f"[bold]Filtering subseslist[/bold]")
    console.print(f"  Input: {input_file}")
    console.print(f"  Output: {output_file}\n")
    
    # Read input data
    df = read_session_data(input_file)
    original_count = len(df)
    
    console.print(f"[cyan]Read {original_count} sessions from input file[/cyan]\n")
    
    # Parse subject/session lists
    subject_list = [s.strip().zfill(2) for s in subjects.split(',')] if subjects else None
    exclude_subject_list = [s.strip().zfill(2) for s in exclude_subjects.split(',')] if exclude_subjects else None
    session_list = [s.strip().zfill(2) for s in sessions.split(',')] if sessions else None
    exclude_session_list = [s.strip().zfill(2) for s in exclude_sessions.split(',')] if exclude_sessions else None
    
    # Apply filters
    filtered_df = filter_sessions(
        df,
        require_dwi=True if (require_dwi or require_all) else None,
        require_floc=True if (require_floc or require_all) else None,
        require_prf=True if (require_prf or require_all) else None,
        require_t1=True if (require_t1 or require_all) else None,
        require_t2=True if (require_t2 or require_all) else None,
        require_levels=require_levels,
        exclude_dwi=True if exclude_dwi else None,
        exclude_floc=True if exclude_floc else None,
        exclude_prf=True if exclude_prf else None,
        exclude_t1=True if exclude_t1 else None,
        exclude_t2=True if exclude_t2 else None,
        exclude_levels=exclude_levels,
        subjects=subject_list,
        exclude_subjects=exclude_subject_list,
        sessions_range=session_list,
        exclude_sessions=exclude_session_list,
    )
    
    # Write output
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    write_subseslist(filtered_df, output_file)
    
    # Print summary
    print_summary(original_count, len(filtered_df), filtered_df)
    
    # Show detailed list if requested
    if show_filtered and len(filtered_df) > 0:
        console.print(f"\n[bold]Filtered Sessions:[/bold]")
        for _, row in filtered_df.sort_values(['sub', 'ses']).iterrows():
            modalities = []
            if row['DWI']:
                modalities.append("DWI")
            if row['func_floc']:
                modalities.append("fLoc")
            if row['func_prf']:
                modalities.append("PRF")
            if row['t1']:
                modalities.append("T1")
            if row['t2']:
                modalities.append("T2")
            
            mod_str = ", ".join(modalities)
            level_str = f"L{int(row['levels'])}" if pd.notna(row['levels']) else "L?"
            console.print(f"  sub-{row['sub']}/ses-{row['ses']}: {mod_str} [{level_str}]")
    
    console.print(f"\n[green]✓ Wrote {len(filtered_df)} sessions to {output_file}[/green]")


if __name__ == "__main__":
    app()