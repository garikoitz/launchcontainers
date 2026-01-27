#!/usr/bin/env python3
"""
Check DICOM file counts in raw data directories.
"""
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Set
from dataclasses import dataclass, field
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
import re

import typer
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn

app = typer.Typer()
console = Console()


@dataclass
class ProtocolCheck:
    """Results for a single protocol/series."""
    protocol_name: str
    series_number: str
    dicom_count: int
    expected_count: Optional[int] = None
    
    @property
    def is_valid(self) -> bool:
        """Check if dicom count matches expected."""
        if self.expected_count is None:
            return True
        return self.dicom_count == self.expected_count
    
    @property
    def protocol_type(self) -> str:
        """Determine protocol type from name."""
        name_lower = self.protocol_name.lower()
        
        if "t1" in name_lower and "mp2rage" in name_lower:
            if "inv1" in name_lower or "inv-1" in name_lower:
                return "t1_INV1"
            elif "inv2" in name_lower or "inv-2" in name_lower:
                return "t1_INV2"
            elif "uni" in name_lower:
                return "t1_UNI"
            return "t1"
        
        if "t2" in name_lower:
            return "t2"
        
        if "dmri" in name_lower or "dwi" in name_lower:
            if "sbref" in name_lower:
                if "pha" in name_lower or "phase" in name_lower:
                    return "dwi_SBRef_Pha"
                return "dwi_SBRef_mag"
            elif "pha" in name_lower or "phase" in name_lower:
                return "dwi_Pha"
            return "dwi_mag"
        
        if "floc" in name_lower:
            if "sbref" in name_lower:
                if "pha" in name_lower or "phase" in name_lower:
                    return "floc_SBRef_Pha"
                return "floc_SBRef_mag"
            elif "pha" in name_lower or "phase" in name_lower:
                return "floc_Pha"
            return "floc_mag"
        
        # Retinotopy and PRF - extract task name (retCB, retFF, retRW, prf_CB, prf_word, etc.)
        if "ret" in name_lower or "prf" in name_lower:
            # Match patterns like: retCB, retFF, retRW, prf_CB, prf_word, etc.
            task_match = re.search(r'(ret[a-z]+|prf[_-]?[a-z]+)', name_lower, re.IGNORECASE)
            if task_match:
                task = task_match.group(0).replace('_', '').replace('-', '')
            else:
                task = "ret"
            
            main_type = f"ret_{task}"
            
            if "sbref" in name_lower:
                if "pha" in name_lower or "phase" in name_lower:
                    return f"{main_type}_SBRef_Pha"
                return f"{main_type}_SBRef_mag"
            elif "pha" in name_lower or "phase" in name_lower:
                return f"{main_type}_Pha"
            return f"{main_type}_mag"
        
        return "unknown"


@dataclass
class SessionCheck:
    """Results for a single session."""
    subject: str
    session: str
    session_dir: Path
    protocols: List[ProtocolCheck] = field(default_factory=list)
    combined_sessions: List[str] = field(default_factory=list)  # Track which physical sessions were combined
    exists: bool = True  # Whether the session directory exists
    
    @property
    def session_id(self) -> str:
        """Get session identifier."""
        return f"sub-{self.subject}/{self.session}"
    
    def debug_protocol_types(self):
        """Debug: print all protocol types found."""
        console.print(f"\n[yellow]DEBUG: Protocol types for {self.session_id}:[/yellow]")
        if self.combined_sessions:
            console.print(f"  [cyan]Combined from physical sessions: {', '.join(self.combined_sessions)}[/cyan]")
        for p in self.protocols:
            console.print(f"  {p.protocol_name}")
            console.print(f"    -> Type: {p.protocol_type}")
            console.print(f"    -> DICOMs: {p.dicom_count}")
    
    def get_protocol_types(self) -> Set[str]:
        """Get set of all protocol types found."""
        types = set()
        for protocol in self.protocols:
            ptype = protocol.protocol_type
            # Normalize ret tasks for checking
            if ptype.startswith("ret"):
                # Extract base type (e.g., "retrw_mag" -> "retrw", "retrw_SBRef_mag" -> "retrw")
                base = ptype.split("_")[0]
                types.add(base)
            else:
                types.add(ptype)
        return types
    
    def has_t1_mp2rage(self) -> Tuple[bool, List[str]]:
        """Check if session has complete T1 MP2RAGE."""
        issues = []
        has_inv1 = any(p.protocol_type == "t1_INV1" for p in self.protocols)
        has_inv2 = any(p.protocol_type == "t1_INV2" for p in self.protocols)
        has_uni = any(p.protocol_type == "t1_UNI" for p in self.protocols)
        
        if not has_inv1:
            issues.append("Missing T1 INV1")
        if not has_inv2:
            issues.append("Missing T1 INV2")
        if not has_uni:
            issues.append("Missing T1 UNI")
        
        return (has_inv1 and has_inv2 and has_uni), issues
    
    def has_t2(self) -> Tuple[bool, List[str]]:
        """Check if session has T2."""
        has_t2 = any(p.protocol_type == "t2" for p in self.protocols)
        issues = [] if has_t2 else ["Missing T2"]
        return has_t2, issues
    
    def has_dwi(self) -> Tuple[bool, List[str]]:
        """Check if session has complete DWI."""
        issues = []
        protocol_types = [p.protocol_type for p in self.protocols]
        
        # Check b06_PA
        has_b06_mag = any("b06" in p.protocol_name.lower() and p.protocol_type == "dwi_mag" for p in self.protocols)
        has_b06_pha = any("b06" in p.protocol_name.lower() and p.protocol_type == "dwi_Pha" for p in self.protocols)
        has_b06_sbref_mag = any("b06" in p.protocol_name.lower() and p.protocol_type == "dwi_SBRef_mag" for p in self.protocols)
        has_b06_sbref_pha = any("b06" in p.protocol_name.lower() and p.protocol_type == "dwi_SBRef_Pha" for p in self.protocols)
        
        if not has_b06_mag:
            issues.append("Missing DWI b06_PA mag")
        if not has_b06_pha:
            issues.append("Missing DWI b06_PA Pha")
        if not has_b06_sbref_mag:
            issues.append("Missing DWI b06_PA SBRef_mag")
        if not has_b06_sbref_pha:
            issues.append("Missing DWI b06_PA SBRef_Pha")
        
        # Check dir104_AP
        has_dir104_mag = any("dir104" in p.protocol_name.lower() and p.protocol_type == "dwi_mag" for p in self.protocols)
        has_dir104_pha = any("dir104" in p.protocol_name.lower() and p.protocol_type == "dwi_Pha" for p in self.protocols)
        has_dir104_sbref_mag = any("dir104" in p.protocol_name.lower() and p.protocol_type == "dwi_SBRef_mag" for p in self.protocols)
        has_dir104_sbref_pha = any("dir104" in p.protocol_name.lower() and p.protocol_type == "dwi_SBRef_Pha" for p in self.protocols)
        
        if not has_dir104_mag:
            issues.append("Missing DWI dir104_AP mag")
        if not has_dir104_pha:
            issues.append("Missing DWI dir104_AP Pha")
        if not has_dir104_sbref_mag:
            issues.append("Missing DWI dir104_AP SBRef_mag")
        if not has_dir104_sbref_pha:
            issues.append("Missing DWI dir104_AP SBRef_Pha")
        
        complete = (has_b06_mag and has_b06_pha and has_b06_sbref_mag and has_b06_sbref_pha and
                   has_dir104_mag and has_dir104_pha and has_dir104_sbref_mag and has_dir104_sbref_pha)
        
        return complete, issues
    
    def has_floc(self) -> Tuple[bool, List[str]]:
        """Check if session has complete fLoc (at least 10 runs)."""
        issues = []
        
        # Count valid runs (mag with correct file count)
        valid_mag_runs = [p for p in self.protocols 
                         if p.protocol_type == "floc_mag" and p.dicom_count == 160]
        valid_pha_runs = [p for p in self.protocols 
                         if p.protocol_type == "floc_Pha" and p.dicom_count == 160]
        valid_sbref_mag = [p for p in self.protocols 
                          if p.protocol_type == "floc_SBRef_mag" and p.dicom_count == 1]
        valid_sbref_pha = [p for p in self.protocols 
                          if p.protocol_type == "floc_SBRef_Pha" and p.dicom_count == 1]
        
        num_valid_mag = len(valid_mag_runs)
        num_valid_pha = len(valid_pha_runs)
        num_valid_sbref_mag = len(valid_sbref_mag)
        num_valid_sbref_pha = len(valid_sbref_pha)
        
        if num_valid_mag < 10:
            issues.append(f"fLoc: Only {num_valid_mag} valid runs (expected at least 10)")
        if num_valid_pha < num_valid_mag:
            issues.append(f"fLoc: Valid Pha runs ({num_valid_pha}) < valid mag runs ({num_valid_mag})")
        if num_valid_sbref_mag < num_valid_mag:
            issues.append(f"fLoc: Valid SBRef_mag ({num_valid_sbref_mag}) < valid mag runs ({num_valid_mag})")
        if num_valid_sbref_pha < num_valid_mag:
            issues.append(f"fLoc: Valid SBRef_Pha ({num_valid_sbref_pha}) < valid mag runs ({num_valid_mag})")
        
        complete = (num_valid_mag >= 10 and num_valid_pha >= num_valid_mag and 
                   num_valid_sbref_mag >= num_valid_mag and num_valid_sbref_pha >= num_valid_mag)
        
        return complete, issues
    
    def has_ret(self) -> Tuple[bool, List[str]]:
        """Check if session has retinotopy scans."""
        issues = []
        
        # Get all ret tasks
        ret_tasks = set()
        for p in self.protocols:
            ptype = p.protocol_type
            if ptype.startswith("ret_"):
                # Extract task name: "ret_retcb_mag" -> "retcb"
                parts = ptype.split("_")
                if len(parts) >= 2:
                    task = parts[1]
                    ret_tasks.add(task)
        
        if not ret_tasks:
            issues.append("Missing retinotopy scans")
            return False, issues
        
        # Check each ret task
        all_tasks_complete = True
        for task in ret_tasks:
            valid_mag = [p for p in self.protocols 
                        if p.protocol_type == f"ret_{task}_mag" and p.dicom_count == 156]
            valid_pha = [p for p in self.protocols 
                        if p.protocol_type == f"ret_{task}_Pha" and p.dicom_count == 156]
            valid_sbref_mag = [p for p in self.protocols 
                              if p.protocol_type == f"ret_{task}_SBRef_mag" and p.dicom_count == 1]
            valid_sbref_pha = [p for p in self.protocols 
                              if p.protocol_type == f"ret_{task}_SBRef_Pha" and p.dicom_count == 1]
            
            num_mag = len(valid_mag)
            num_pha = len(valid_pha)
            num_sbref_mag = len(valid_sbref_mag)
            num_sbref_pha = len(valid_sbref_pha)
            
            task_display = task.upper()
            
            # At least one complete run for this task
            if num_mag < 1:
                issues.append(f"{task_display}: No valid mag runs")
                all_tasks_complete = False
            if num_pha < num_mag:
                issues.append(f"{task_display}: Valid Pha runs ({num_pha}) < valid mag runs ({num_mag})")
                all_tasks_complete = False
            if num_sbref_mag < num_mag:
                issues.append(f"{task_display}: Valid SBRef_mag ({num_sbref_mag}) < valid mag runs ({num_mag})")
                all_tasks_complete = False
            if num_sbref_pha < num_mag:
                issues.append(f"{task_display}: Valid SBRef_Pha ({num_sbref_pha}) < valid mag runs ({num_mag})")
                all_tasks_complete = False
        
        # Complete if at least one ret task has at least one complete run
        has_at_least_one_complete = any(
            len([p for p in self.protocols if p.protocol_type == f"ret_{task}_mag" and p.dicom_count == 156]) >= 1
            for task in ret_tasks
        )
        
        return has_at_least_one_complete and all_tasks_complete, issues
    
    def get_file_count_issues(self) -> List[str]:
        """Get issues related to incorrect file counts."""
        issues = []
        
        for protocol in self.protocols:
            expected = protocol.get_expected_count()
            if expected is not None and protocol.dicom_count != expected:
                issues.append(
                    f"{protocol.protocol_name}: Has {protocol.dicom_count} DICOMs (expected {expected})"
                )
        
        return issues
    
    def get_issues(self) -> List[str]:
        """Get list of all issues for this session."""
        if not self.exists:
            return ["Session directory does not exist"]
        
        issues = []
        
        # Check modality presence
        has_t1, t1_issues = self.has_t1_mp2rage()
        issues.extend(t1_issues)
        
        has_t2, t2_issues = self.has_t2()
        issues.extend(t2_issues)
        
        has_dwi, dwi_issues = self.has_dwi()
        issues.extend(dwi_issues)
        
        has_floc, floc_issues = self.has_floc()
        issues.extend(floc_issues)
        
        has_ret, ret_issues = self.has_ret()
        issues.extend(ret_issues)
        
        # Check file counts (only for protocols that exist)
        # issues.extend(self.get_file_count_issues())
        
        return issues
    
    @property
    def is_complete(self) -> bool:
        """Check if session is complete."""
        return self.exists and len(self.get_issues()) == 0


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


def find_dicom_directories(raw_data_dir: Path, subject: str, session: str) -> List[Path]:
    """
    Find all DICOM protocol directories for a session.
    
    Expected structures:
      1. raw_data/sub-XX/ses-XX/protocol_name/*.dcm
      2. raw_data/sub-XX/ses-XX/middle_dir1/middle_dir2/protocol_name/*.dcm
    
    This function searches up to 2 levels deep for protocol directories.
    """
    dicom_dirs = []
    
    # Try different possible session path structures
    possible_session_paths = [
        raw_data_dir / f"sub-{subject}" / session,
        raw_data_dir / f"sub-{subject}_{session}",
        raw_data_dir / subject / session,
    ]
    
    for session_path in possible_session_paths:
        if not session_path.exists() or not session_path.is_dir():
            continue
        
        # Level 1: Check if protocol directories are directly under session
        for item in session_path.iterdir():
            if not item.is_dir():
                continue
            
            # Check if this directory contains .dcm files (it's a protocol directory)
            dcm_files = list(item.glob("*.dcm"))
            if dcm_files:
                dicom_dirs.append(item)
            else:
                # Level 2: Check one level deeper
                for subitem in item.iterdir():
                    if not subitem.is_dir():
                        continue
                    
                    # Check if this directory contains .dcm files
                    dcm_files = list(subitem.glob("*.dcm"))
                    if dcm_files:
                        dicom_dirs.append(subitem)
                    else:
                        # Level 3: Check two levels deeper
                        for subsubitem in subitem.iterdir():
                            if not subsubitem.is_dir():
                                continue
                            
                            # Check if this directory contains .dcm files
                            dcm_files = list(subsubitem.glob("*.dcm"))
                            if dcm_files:
                                dicom_dirs.append(subsubitem)
        
        # If we found any dicom directories, stop searching other session path formats
        if dicom_dirs:
            break
    
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
    
    # Find all protocol directories
    protocol_dirs = find_dicom_directories(raw_data_dir, subject, session) if exists else []
    
    protocols = []
    for protocol_dir in protocol_dirs:
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
        exists=exists
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
        for session in session_list:
            if (subject, session) in physical_results:
                result = physical_results[(subject, session)]
                all_protocols.extend(result.protocols)
                if result.exists:
                    exists = True
        
        # Create combined session result
        combined_result = SessionCheck(
            subject=subject,
            session=base_session,
            session_dir=raw_data_dir / f"sub-{subject}" / base_session,
            protocols=all_protocols,
            combined_sessions=session_list,
            exists=exists
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
    found_sessions = {(r.subject, r.session) for r in results}
    missing_sessions = expected_sessions - found_sessions
    
    # Categorize results
    complete = [r for r in results if r.is_complete and r.exists]
    incomplete = [r for r in results if not r.is_complete and r.exists]
    
    # Export complete sessions
    complete_file = output_dir / "complete_sessions.txt"
    with open(complete_file, 'w') as f:
        f.write("sub\tses\n")
        for r in sorted(complete, key=lambda x: (x.subject, x.session)):
            f.write(f"sub-{r.subject}\t{r.session}\n")
    console.print(f"[green]✓ Exported complete sessions ({len(complete)}): {complete_file}[/green]")
    
    # Export incomplete sessions
    incomplete_file = output_dir / "incomplete_sessions.txt"
    with open(incomplete_file, 'w') as f:
        f.write("sub\tses\tissues\n")
        for r in sorted(incomplete, key=lambda x: (x.subject, x.session)):
            issues = "; ".join(r.get_issues())
            f.write(f"sub-{r.subject}\t{r.session}\t{issues}\n")
    console.print(f"[yellow]⚠ Exported incomplete sessions ({len(incomplete)}): {incomplete_file}[/yellow]")
    
    # Export missing sessions
    missing_file = output_dir / "missing_sessions.txt"
    with open(missing_file, 'w') as f:
        f.write("sub\tses\n")
        for subject, session in sorted(missing_sessions):
            f.write(f"sub-{subject}\t{session}\n")
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


# ... (print_session_details, print_dwi_component, print_functional_runs remain the same)
# I'll include them at the end for completeness

            
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
        help="Directory to save session list files (complete, incomplete, missing)"
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
    
    # Export session lists if output directory specified
    if output_dir:
        export_session_lists(results, output_dir)


if __name__ == "__main__":
    app()