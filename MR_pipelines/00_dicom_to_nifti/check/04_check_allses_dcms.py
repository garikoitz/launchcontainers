#!/usr/bin/env python3
"""
Check DICOM file counts in raw data directories.
"""
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Set
from collections import defaultdict, Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
import re
import csv
import sys

import typer
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn

# Add the project root to Python path
script_dir = Path(__file__).resolve().parent
if str(script_dir) not in sys.path:
    sys.path.insert(0, str(script_dir))

from launchcontainers.votcloc.dicom_checks import ProtocolCheck, SessionCheck

app = typer.Typer()
console = Console()


def get_expected_sessions(num_subjects: int = 11, num_sessions: int = 10) -> Set[Tuple[str, str]]:
    """
    Get expected sessions for the study.
    
    Args:
        num_subjects: Number of subjects (default: 11, subjects 01-11)
        num_sessions: Number of sessions per subject (default: 10, sessions 01-10)
    
    Returns:
        Set of (subject, session) tuples
    """
    expected = set()
    for sub_num in range(1, num_subjects + 1):
        for ses_num in range(1, num_sessions + 1):
            subject = f"{sub_num:02d}"
            session = f"ses-{ses_num:02d}"
            expected.add((subject, session))
    return expected


def parse_session_id(session: str) -> Tuple[str, str]:
    """
    Parse session ID into base session and suffix.
    
    Examples:
        "ses-02" -> ("ses-02", "")
        "ses-02part1" -> ("ses-02", "part1")
        "ses-04part1june26" -> ("ses-04", "part1june26")
    
    Returns:
        Tuple of (base_session, suffix)
    """
    # Extract the numeric part after "ses-"
    match = re.match(r'(ses-\d{2})(.*)', session)
    if match:
        base = match.group(1)
        suffix = match.group(2)
        return base, suffix
    return session, ""


def calculate_dicom_depth(protocol_dir: Path, session_dir: Path) -> int:
    """
    Calculate how many directory levels deep the DICOM files are from the session directory.
    
    Args:
        protocol_dir: Path to the protocol directory containing .dcm files
        session_dir: Path to the session directory
    
    Returns:
        Number of directory levels (e.g., 2 for ses-01/protocol/*.dcm, 4 for ses-01/a/b/protocol/*.dcm)
    """
    try:
        relative_path = protocol_dir.relative_to(session_dir)
        # Count the number of directory components
        # +1 because we count the protocol directory itself
        return len(relative_path.parts)
    except ValueError:
        # If paths are not relative, return None
        return None


def find_dicom_directories(raw_data_dir: Path, subject: str, session: str) -> List[Tuple[Path, int]]:
    """
    Find all DICOM protocol directories for a session and their depths.
    
    Expected structures:
      1. raw_data/sub-XX/ses-XX/protocol_name/*.dcm (depth=1)
      2. raw_data/sub-XX/ses-XX/middle_dir1/middle_dir2/protocol_name/*.dcm (depth=3)
    
    Returns:
        List of tuples (protocol_directory_path, depth_from_session)
    """
    dicom_dirs = []
    
    # Try different possible session path structures
    possible_session_paths = [
        raw_data_dir / f"sub-{subject}" / session,
        raw_data_dir / f"sub-{subject}_{session}",
        raw_data_dir / subject / session,
    ]
    
    session_dir = None
    for session_path in possible_session_paths:
        if session_path.exists() and session_path.is_dir():
            session_dir = session_path
            break
    
    if session_dir is None:
        return []
    
    # Level 1: Check if protocol directories are directly under session
    for item in session_dir.iterdir():
        if not item.is_dir():
            continue
        
        # Check if this directory contains .dcm files (it's a protocol directory)
        dcm_files = list(item.glob("*.dcm"))
        if dcm_files:
            depth = calculate_dicom_depth(item, session_dir)
            dicom_dirs.append((item, depth))
        else:
            # Level 2: Check one level deeper
            for subitem in item.iterdir():
                if not subitem.is_dir():
                    continue
                
                # Check if this directory contains .dcm files
                dcm_files = list(subitem.glob("*.dcm"))
                if dcm_files:
                    depth = calculate_dicom_depth(subitem, session_dir)
                    dicom_dirs.append((subitem, depth))
                else:
                    # Level 3: Check two levels deeper
                    for subsubitem in subitem.iterdir():
                        if not subsubitem.is_dir():
                            continue
                        
                        # Check if this directory contains .dcm files
                        dcm_files = list(subsubitem.glob("*.dcm"))
                        if dcm_files:
                            depth = calculate_dicom_depth(subsubitem, session_dir)
                            dicom_dirs.append((subsubitem, depth))
    
    return dicom_dirs


def count_dicoms_in_directory(protocol_dir: Path) -> Tuple[int, str]:
    """
    Count DICOM files in a directory.
    
    Returns:
        Tuple of (dicom_count, series_number)
    """
    # Count .dcm files
    dicom_files = list(protocol_dir.glob("*.dcm"))
    
    # Try to extract series number from directory name or first file
    series_number = "unknown"
    dir_name = protocol_dir.name
    
    # Try to extract from directory name (e.g., "3-fLoc_run_01")
    series_match = re.match(r'^(\d+)-', dir_name)
    if series_match:
        series_number = series_match.group(1)
    
    return len(dicom_files), series_number


def check_session(args: Tuple[Path, str, str]) -> SessionCheck:
    """Check a single session's DICOM files (for parallel processing)."""
    raw_data_dir, subject, session = args
    session_dir = raw_data_dir / f"sub-{subject}" / session
    
    # Check if session exists
    exists = session_dir.exists()
    
    if not exists:
        # Check for alternative path formats
        alt_paths = [
            raw_data_dir / f"sub-{subject}_{session}",
            raw_data_dir / subject / session,
        ]
        for alt_path in alt_paths:
            if alt_path.exists():
                exists = True
                session_dir = alt_path
                break
    
    # Find all protocol directories with their depths
    protocol_dirs_with_depth = find_dicom_directories(raw_data_dir, subject, session) if exists else []
    
    # Get the most common depth (mode) or first depth if all protocols at same depth
    depths = [depth for _, depth in protocol_dirs_with_depth if depth is not None]
    if depths:
        # Use the most common depth value
        depth_counts = Counter(depths)
        avg_depth = depth_counts.most_common(1)[0][0]  # Get the most common depth
    else:
        avg_depth = None
    
    protocols = []
    for protocol_dir, depth in protocol_dirs_with_depth:
        dicom_count, series_number = count_dicoms_in_directory(protocol_dir)
        
        protocol = ProtocolCheck(
            protocol_name=protocol_dir.name,
            series_number=series_number,
            dicom_count=dicom_count
        )
        protocols.append(protocol)
    
    return SessionCheck(
        subject=subject,
        session=session,
        session_dir=session_dir,
        protocols=protocols,
        exists=exists,
        dicom_depth=avg_depth
    )


def get_sessions_from_subseslist(subseslist_file: Path) -> List[Tuple[str, str]]:
    """
    Read sessions from subseslist.txt file.
    
    Expected format:
        sub    ses
        sub-01 ses-01
        sub-01 ses-02
    """
    sessions = []
    
    with open(subseslist_file, 'r') as f:
        lines = f.readlines()
        
        # Skip header
        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue
            
            parts = line.split()
            if len(parts) >= 2:
                sub = parts[0].replace('sub-', '')
                ses = parts[1]
                sessions.append((sub, ses))
    
    return sessions


def combine_sessions_by_base(sessions: List[Tuple[str, str]]) -> Dict[Tuple[str, str], List[str]]:
    """
    Group sessions by their base session ID.
    
    For example:
        ("01", "ses-02") and ("01", "ses-02part1") -> {("01", "ses-02"): ["ses-02", "ses-02part1"]}
    
    Returns:
        Dict mapping (subject, base_session) to list of physical session IDs
    """
    grouped = defaultdict(list)
    
    for subject, session in sessions:
        base_session, suffix = parse_session_id(session)
        grouped[(subject, base_session)].append(session)
    
    return grouped


def check_all_sessions(raw_data_dir: Path, subseslist_file: Optional[Path] = None, 
                      n_jobs: int = 30, check_expected: bool = True) -> List[SessionCheck]:
    """Check all sessions with parallel processing and a progress bar."""
    
    # Get expected sessions
    expected_sessions = get_expected_sessions() if check_expected else set()
    
    if subseslist_file:
        physical_sessions = get_sessions_from_subseslist(subseslist_file)
    else:
        # Auto-detect sessions
        physical_sessions = []
        for sub_dir in sorted(raw_data_dir.iterdir()):
            if not sub_dir.is_dir() or not sub_dir.name.startswith('sub-'):
                continue
            
            subject = sub_dir.name.replace('sub-', '')
            
            for ses_dir in sorted(sub_dir.iterdir()):
                if not ses_dir.is_dir() or not ses_dir.name.startswith('ses-'):
                    continue
                
                session = ses_dir.name
                physical_sessions.append((subject, session))
    
    # Add expected sessions that might not exist
    if check_expected:
        existing_bases = set()
        for subject, session in physical_sessions:
            base_session, _ = parse_session_id(session)
            existing_bases.add((subject, base_session))
        
        # Add missing expected sessions to check list
        for subject, session in expected_sessions:
            if (subject, session) not in existing_bases:
                physical_sessions.append((subject, session))
    
    # Group sessions by base session ID
    grouped_sessions = combine_sessions_by_base(physical_sessions)
    
    # Prepare arguments for parallel processing (process each physical session)
    args_list = [(raw_data_dir, subject, session) for subject, session in physical_sessions]
    
    # First, collect all physical session results
    physical_results = {}
    
    # Create progress bar
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
        console=console,
        transient=False
    ) as progress:
        
        task = progress.add_task(
            "[cyan]Checking DICOM counts...",
            total=len(physical_sessions)
        )
        
        # Use ProcessPoolExecutor for parallel processing
        with ProcessPoolExecutor(max_workers=n_jobs) as executor:
            # Submit all tasks
            future_to_session = {
                executor.submit(check_session, args): args[1:] for args in args_list
            }
            
            # Process results as they complete
            for future in as_completed(future_to_session):
                subject, session = future_to_session[future]
                session_id = f"sub-{subject}/{session}"
                
                try:
                    result = future.result()
                    physical_results[(subject, session)] = result
                    
                    # Update progress
                    progress.update(task, advance=1, 
                                  description=f"[cyan]Checking DICOM counts... {session_id}")
                
                except Exception as exc:
                    console.print(f"[red]✗ Error checking {session_id}: {exc}[/red]")
                    progress.update(task, advance=1)
    
    # Now combine sessions that share the same base session ID
    combined_results = []
    
    for (subject, base_session), session_list in sorted(grouped_sessions.items()):
        # Collect all protocols from all physical sessions
        all_protocols = []
        exists = False
        all_depths = []
        
        for session in session_list:
            if (subject, session) in physical_results:
                result = physical_results[(subject, session)]
                all_protocols.extend(result.protocols)
                if result.exists:
                    exists = True
                if result.dicom_depth is not None:
                    all_depths.append(result.dicom_depth)
        
        # Use the most common depth value
        if all_depths:
            depth_counts = Counter(all_depths)
            avg_depth = depth_counts.most_common(1)[0][0]  # Get the most common depth
        else:
            avg_depth = None
        
        # Create combined session result
        combined_result = SessionCheck(
            subject=subject,
            session=base_session,
            session_dir=raw_data_dir / f"sub-{subject}" / base_session,
            protocols=all_protocols,
            combined_sessions=session_list,
            exists=exists,
            dicom_depth=avg_depth
        )
        
        combined_results.append(combined_result)
        
        # Print completion message
        session_id = f"sub-{subject}/{base_session}"
        if not exists:
            console.print(f"[red]✗ Missing: {session_id}[/red]")
        elif len(session_list) > 1:
            console.print(f"[blue]ℹ Combined: {session_id} (from {', '.join(session_list)})[/blue]")
            if combined_result.is_complete:
                console.print(f"[green]✓ Checked: {session_id}[/green]")
            else:
                console.print(f"[yellow]⚠ Checked: {session_id} (has issues)[/yellow]")
        else:
            if combined_result.is_complete:
                console.print(f"[green]✓ Checked: {session_id}[/green]")
            else:
                console.print(f"[yellow]⚠ Checked: {session_id} (has issues)[/yellow]")
    
    # Sort results by session_id for consistent output
    combined_results.sort(key=lambda x: x.session_id)
    
    return combined_results


def export_csv_summary(results: List[SessionCheck], output_file: Path):
    """
    Export session summary to CSV file.
    
    Columns: sub, ses, DWI, func_floc, func_prf, t1, t2, levels
    """
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['sub', 'ses', 'DWI', 'func_floc', 'func_prf', 't1', 't2', 'levels']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        
        for result in sorted(results, key=lambda x: (x.subject, x.session)):
            # Check modalities
            has_dwi, _ = result.has_dwi()
            has_floc, _ = result.has_floc()
            has_ret, _ = result.has_ret()  # Using ret as proxy for PRF
            has_t1, _ = result.has_t1_mp2rage()
            has_t2, _ = result.has_t2()
            
            # Format depth as integer
            depth_str = str(int(result.dicom_depth)) if result.dicom_depth is not None else ""
            
            writer.writerow({
                'sub': result.subject,  # No 'sub-' prefix
                'ses': result.session.replace('ses-', ''),  # Remove 'ses-' prefix
                'DWI': str(has_dwi),
                'func_floc': str(has_floc),
                'func_prf': str(has_ret),
                't1': str(has_t1),
                't2': str(has_t2),
                'levels': depth_str
            })
    
    console.print(f"[green]✓ Exported CSV summary: {output_file}[/green]")


def print_expected_session_check(results: List[SessionCheck]):
    """Print check of expected 110 sessions (11 subjects × 10 sessions)."""
    expected_sessions = get_expected_sessions()
    
    found_sessions = {(r.subject, r.session) for r in results}
    missing_sessions = expected_sessions - found_sessions
    
    console.print(f"\n[bold]Expected Session Check (11 subjects × 10 sessions = 110 total):[/bold]")
    console.print(f"  Expected: {len(expected_sessions)}")
    console.print(f"  Found: {len(found_sessions)}")
    console.print(f"  [red]Missing: {len(missing_sessions)}[/red]")
    
    if missing_sessions:
        console.print(f"\n[bold red]Missing Sessions:[/bold red]")
        for subject, session in sorted(missing_sessions):
            console.print(f"  sub-{subject}/{session}")


def export_session_lists(results: List[SessionCheck], output_dir: Path):
    """
    Export three files: complete sessions, incomplete sessions, and missing sessions.
    
    Args:
        results: List of session check results
        output_dir: Directory to save the output files
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get expected sessions
    expected_sessions = get_expected_sessions()
    found_sessions = {(r.subject, r.session) for r in results if r.exists}
    missing_sessions = expected_sessions - found_sessions
    
    # Categorize results - only include sessions that exist
    complete = [r for r in results if r.is_complete and r.exists]
    incomplete = [r for r in results if not r.is_complete and r.exists]
    
    # Export complete sessions
    complete_file = output_dir / "complete_sessions.txt"
    with open(complete_file, 'w') as f:
        f.write("sub,ses\n")
        for r in sorted(complete, key=lambda x: (x.subject, x.session)):
            f.write(f"{r.subject},{r.session.replace('ses-', '')}\n")
    console.print(f"[green]✓ Exported complete sessions ({len(complete)}): {complete_file}[/green]")
    
    # Export incomplete sessions
    incomplete_file = output_dir / "incomplete_sessions.txt"
    with open(incomplete_file, 'w') as f:
        f.write("sub,ses,issues\n")
        for r in sorted(incomplete, key=lambda x: (x.subject, x.session)):
            issues = "; ".join(r.get_issues())
            f.write(f"{r.subject},{r.session.replace('ses-', '')},{issues}\n")
    console.print(f"[yellow]⚠ Exported incomplete sessions ({len(incomplete)}): {incomplete_file}[/yellow]")
    
    # Export missing sessions
    missing_file = output_dir / "missing_sessions.txt"
    with open(missing_file, 'w') as f:
        f.write("sub,ses\n")
        for subject, session in sorted(missing_sessions):
            f.write(f"{subject},{session.replace('ses-', '')}\n")
    console.print(f"[red]✗ Exported missing sessions ({len(missing_sessions)}): {missing_file}[/red]")
    
    # Summary file
    summary_file = output_dir / "session_summary.txt"
    with open(summary_file, 'w') as f:
        f.write("DICOM Session Check Summary\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Expected sessions (11 subjects × 10 sessions): 110\n")
        f.write(f"Found sessions: {len(found_sessions)}\n")
        f.write(f"Complete sessions: {len(complete)}\n")
        f.write(f"Incomplete sessions: {len(incomplete)}\n")
        f.write(f"Missing sessions: {len(missing_sessions)}\n\n")
        
        f.write("Files generated:\n")
        f.write(f"  - complete_sessions.txt: {len(complete)} sessions\n")
        f.write(f"  - incomplete_sessions.txt: {len(incomplete)} sessions\n")
        f.write(f"  - missing_sessions.txt: {len(missing_sessions)} sessions\n")
        f.write(f"  - session_data.txt: TXT format with modality flags\n")
    console.print(f"[blue]ℹ Exported summary: {summary_file}[/blue]")


def print_summary(results: List[SessionCheck]):
    """Print summary statistics."""
    total = len(results)
    complete = sum(1 for r in results if r.is_complete)
    incomplete = sum(1 for r in results if not r.is_complete and r.exists)
    missing = sum(1 for r in results if not r.exists)
    
    # Count combined sessions
    combined_count = sum(1 for r in results if len(r.combined_sessions) > 1)
    
    console.print(f"\n[bold]Summary:[/bold]")
    console.print(f"  Total logical sessions: {total}")
    if combined_count > 0:
        console.print(f"  Combined sessions: {combined_count}")
    console.print(f"  [green]✓ Complete sessions: {complete}[/green]")
    console.print(f"  [yellow]⚠ Incomplete sessions: {incomplete}[/yellow]")
    console.print(f"  [red]✗ Missing sessions: {missing}[/red]")


def print_modality_summary(results: List[SessionCheck]):
    """Print summary of modality completeness."""
    console.print(f"\n[bold]Modality Summary:[/bold]")
    
    # Only count sessions that exist
    existing_results = [r for r in results if r.exists]
    total = len(existing_results)
    
    if total == 0:
        console.print("  [yellow]No existing sessions found[/yellow]")
        return
    
    # Count sessions with each modality
    has_t1 = sum(1 for r in existing_results if r.has_t1_mp2rage()[0])
    has_t2 = sum(1 for r in existing_results if r.has_t2()[0])
    has_dwi = sum(1 for r in existing_results if r.has_dwi()[0])
    has_floc = sum(1 for r in existing_results if r.has_floc()[0])
    has_ret = sum(1 for r in existing_results if r.has_ret()[0])
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Modality", style="cyan")
    table.add_column("Complete Sessions", style="green")
    table.add_column("Coverage", style="blue")
    
    modalities = [
        ("T1 MP2RAGE", has_t1),
        ("T2", has_t2),
        ("DWI", has_dwi),
        ("fLoc (≥10 runs)", has_floc),
        ("Retinotopy", has_ret),
    ]
    
    for mod_name, count in modalities:
        coverage = f"{count}/{total} ({100*count/total:.0f}%)" if total > 0 else "N/A"
        table.add_row(mod_name, str(count), coverage)
    
    console.print(table)


def print_detailed_issues(results: List[SessionCheck]):
    """Print detailed issues."""
    incomplete = [r for r in results if not r.is_complete]
    
    if not incomplete:
        console.print("\n[bold green]✓ All sessions complete with all required modalities![/bold green]")
        return
    
    console.print(f"\n[bold red]Incomplete Sessions ({len(incomplete)}):[/bold red]\n")
    
    # Group by issue type
    by_category = defaultdict(list)
    for result in incomplete:
        issues = result.get_issues()
        for issue in issues:
            # Extract category (first part before colon or first word)
            if ":" in issue:
                category = issue.split(":")[0]
            else:
                category = issue.split()[0]
            by_category[category].append((result, issue))
    
    # Print by category
    for category in sorted(by_category.keys()):
        items = by_category[category]
        unique_sessions = set(item[0].session_id for item in items)
        
        console.print(f"[yellow]{category} issues ({len(unique_sessions)} sessions):[/yellow]")
        
        # Group by session
        by_session = defaultdict(list)
        for result, issue in items:
            by_session[result.session_id].append(issue)
        
        for session_id in sorted(by_session.keys())[:10]:  # Show first 10
            console.print(f"  {session_id}:")
            for issue in by_session[session_id]:
                console.print(f"    - {issue}")
        
        if len(by_session) > 10:
            console.print(f"  ... and {len(by_session) - 10} more sessions")
        console.print()


@app.command()
def main(
    raw_data_dir: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=False,
        dir_okay=True,
        help="Path to raw DICOM data directory"
    ),
    subseslist: Optional[Path] = typer.Option(
        None,
        "--subseslist",
        "-l",
        help="Path to subseslist.txt file (optional, will auto-detect if not provided)"
    ),
    n_jobs: int = typer.Option(
        30,
        "--jobs",
        "-j",
        help="Number of parallel jobs to run"
    ),
    show_complete: bool = typer.Option(
        False,
        "--show-complete",
        "-c",
        help="Show details for complete sessions too"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show all protocols including complete ones with DICOM counts"
    ),
    debug_session: Optional[str] = typer.Option(
        None,
        "--debug",
        "-d",
        help="Debug a specific session (e.g., 'sub-01/ses-02')"
    ),
    output_dir: Optional[Path] = typer.Option(
        None,
        "--output-dir",
        "-o",
        help="Directory to save output files (session lists and CSV summary)"
    )
):
    """
    Check DICOM file counts and modality completeness in raw data directories.
    
    Expected: 11 subjects (01-11) × 10 sessions (01-10) = 110 sessions total
    
    Sessions with suffixes (e.g., ses-02part1, ses-02part2) are automatically combined
    and treated as a single logical session (ses-02).
    
    Expected structure:
      raw_data/sub-XX/ses-XX/protocol_name/*.dcm
      OR
      raw_data/sub-XX/ses-XX/middle_dir1/middle_dir2/protocol_name/*.dcm
    
    Required modalities per session:
    - T1 MP2RAGE: INV1, INV2, UNI
    - T2: 1 series
    - DWI: b06_PA and dir104_AP (each with mag, Pha, SBRef_mag, SBRef_Pha)
    - fLoc: At least 10 runs (each with mag-160, Pha-160, SBRef_mag-1, SBRef_Pha-1)
    - Retinotopy/PRF: At least 1 task (retCB, retFF, retRW, prf_CB, prf_word, etc.)
      (each with mag-156, Pha-156, SBRef_mag-1, SBRef_Pha-1)
    
    Example:
        check_dicom_counts.py /path/to/raw_data
        check_dicom_counts.py /path/to/raw_data --output-dir ./results
        check_dicom_counts.py /path/to/raw_data --verbose
        check_dicom_counts.py /path/to/raw_data --debug sub-01/ses-02
    """
    console.print(f"[bold]Checking DICOM file counts and modalities[/bold]")
    console.print(f"  Raw data dir: {raw_data_dir}")
    
    if not debug_session:
        console.print(f"  Parallel jobs: {n_jobs}")
        console.print(f"  Verbose mode: {verbose}\n")
    
    results = check_all_sessions(raw_data_dir, subseslist, n_jobs, check_expected=True)
    
    if not results:
        console.print("[yellow]No sessions found to check.[/yellow]")
        return
    
    # Debug mode
    if debug_session:
        debug_result = [r for r in results if r.session_id == debug_session]
        if debug_result:
            debug_result[0].debug_protocol_types()
            console.print(f"\n[yellow]Issues:[/yellow]")
            issues = debug_result[0].get_issues()
            if issues:
                for issue in issues:
                    console.print(f"  - {issue}")
            else:
                console.print(f"  [green]No issues found - session is complete![/green]")
        else:
            console.print(f"[red]Session {debug_session} not found[/red]")
        return
    
    print_expected_session_check(results)
    print_summary(results)
    print_modality_summary(results)
    print_detailed_issues(results)
    
    # Export files if output directory specified
    if output_dir:
        export_session_lists(results, output_dir)
        
        # Export CSV summary
        csv_file = output_dir / "session_data.txt"
        export_csv_summary(results, csv_file)


if __name__ == "__main__":
    app()