#!/usr/bin/env python3
"""
Check HeuDiConv DICOM info files for completeness.
"""
from pathlib import Path
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
import csv
import re

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer()
console = Console()


@dataclass
class SeriesInfo:
    """Information about a single series."""
    series_id: str
    dcm_dir_name: str
    series_files: int
    series_description: str = ""
    
    def get_series_type(self) -> Tuple[str, str]:
        """
        Get series type and subtype.
        
        Returns:
            Tuple of (main_type, sub_type)
            e.g., ("t1", "INV1"), ("floc", "mag"), ("ret_retRW", "mag")
        """
        name_lower = self.dcm_dir_name.lower()
        
        # T1 MP2RAGE
        if "t1" in name_lower and "mp2rage" in name_lower:
            if "inv1" in name_lower or "inv-1" in name_lower:
                return ("t1", "INV1")
            elif "inv2" in name_lower or "inv-2" in name_lower:
                return ("t1", "INV2")
            elif "uni" in name_lower:
                return ("t1", "UNI")
        
        # T2
        if "t2" in name_lower:
            return ("t2", "main")
        
        # DWI
        if "dmri" in name_lower or "dwi" in name_lower:
            main_type = "dwi"
            
            # Determine direction
            if "b0" in name_lower or "b06" in name_lower:
                direction = "b06_PA"
            elif "dir104" in name_lower:
                direction = "dir104_AP"
            else:
                direction = "unknown"
            
            # Determine subtype
            if "sbref" in name_lower:
                if "pha" in name_lower or "phase" in name_lower:
                    return (main_type, f"{direction}_SBRef_Pha")
                else:
                    return (main_type, f"{direction}_SBRef_mag")
            elif "pha" in name_lower or "phase" in name_lower:
                return (main_type, f"{direction}_Pha")
            else:
                return (main_type, f"{direction}_mag")
        
        # fLoc
        if "floc" in name_lower:
            if "sbref" in name_lower:
                if "pha" in name_lower or "phase" in name_lower:
                    return ("floc", "SBRef_Pha")
                else:
                    return ("floc", "SBRef_mag")
            elif "pha" in name_lower or "phase" in name_lower:
                return ("floc", "Pha")
            else:
                return ("floc", "mag")
        
        # Retinotopy - extract task name (retRW, retCB, retFF, etc.)
        if "ret" in name_lower:
            # Extract ret task name
            ret_match = re.search(r'ret[a-z]{2,4}', name_lower, re.IGNORECASE)
            if ret_match:
                ret_task = ret_match.group(0)
            else:
                ret_task = "ret"
            
            main_type = f"ret_{ret_task}"
            
            if "sbref" in name_lower:
                if "pha" in name_lower or "phase" in name_lower:
                    return (main_type, "SBRef_Pha")
                else:
                    return (main_type, "SBRef_mag")
            elif "pha" in name_lower or "phase" in name_lower:
                return (main_type, "Pha")
            else:
                return (main_type, "mag")
        
        return ("unknown", "unknown")


@dataclass
class SessionCheck:
    """Results for a single session."""
    subject: str
    session: str
    info_file: Path
    file_exists: bool
    series: List[SeriesInfo] = field(default_factory=list)
    
    @property
    def session_id(self) -> str:
        """Get session identifier."""
        return f"sub-{self.subject}/{self.session}"
    
    def has_successful_rerun(self, series_type: Tuple[str, str], expected_files: int) -> bool:
        """
        Check if there's a successful rerun with the correct number of files.
        
        Args:
            series_type: (main_type, sub_type) tuple
            expected_files: Expected number of files
            
        Returns:
            True if a successful rerun exists
        """
        # Find all series with the same type
        matching_series = [s for s in self.series if s.get_series_type() == series_type]
        
        # Check if any has the correct number of files
        return any(s.series_files == expected_files for s in matching_series)
    
    def check_t1_mp2rage(self) -> List[str]:
        """Check T1 MP2RAGE completeness."""
        issues = []
        t1_series = [s for s in self.series if s.get_series_type()[0] == "t1"]
        
        # Group by subtype
        by_subtype = defaultdict(list)
        for s in t1_series:
            _, subtype = s.get_series_type()
            by_subtype[subtype].append(s)
        
        # Check for required subtypes
        for required in ["INV1", "INV2", "UNI"]:
            if required not in by_subtype:
                issues.append(f"T1: Missing {required}")
            else:
                # Check if any series has valid file count
                valid_series = [s for s in by_subtype[required] if s.series_files >= 1]
                if not valid_series:
                    issues.append(f"T1 {required}: No valid series (all have <1 files)")
        
        return issues
    
    def check_t2(self) -> List[str]:
        """Check T2 completeness."""
        issues = []
        t2_series = [s for s in self.series if s.get_series_type()[0] == "t2"]
        
        if len(t2_series) == 0:
            issues.append("T2: Missing")
        else:
            # Check if any series has valid file count
            valid_series = [s for s in t2_series if s.series_files >= 1]
            if not valid_series:
                issues.append(f"T2: No valid series (all have <1 files)")
        
        return issues
    
    def check_dwi(self) -> List[str]:
        """Check DWI completeness."""
        issues = []
        dwi_series = [s for s in self.series if s.get_series_type()[0] == "dwi"]
        
        # Group by subtype
        by_subtype = defaultdict(list)
        for s in dwi_series:
            _, subtype = s.get_series_type()
            by_subtype[subtype].append(s)
        
        # Expected structure for each direction
        expected_structure = {
            "b06_PA": {
                "b06_PA_mag": 6,
                "b06_PA_Pha": 6,
                "b06_PA_SBRef_mag": 1,
                "b06_PA_SBRef_Pha": 1,
            },
            "dir104_AP": {
                "dir104_AP_mag": 105,
                "dir104_AP_Pha": 105,
                "dir104_AP_SBRef_mag": 1,
                "dir104_AP_SBRef_Pha": 1,
            }
        }
        
        for direction, expected in expected_structure.items():
            for subtype, expected_files in expected.items():
                if subtype not in by_subtype:
                    issues.append(f"DWI: Missing {subtype}")
                else:
                    # Check if any series has the correct file count (considering reruns)
                    valid_series = [s for s in by_subtype[subtype] if s.series_files == expected_files]
                    if not valid_series:
                        # No valid series found
                        actual_files = [s.series_files for s in by_subtype[subtype]]
                        issues.append(f"DWI {subtype}: Has {actual_files} files (expected {expected_files}, no successful rerun)")
        
        return issues
    
    def check_functional(self, func_type_prefix: str, min_runs: int = 10, expected_files: int = 160) -> List[str]:
        """
        Check functional scan completeness (fLoc or ret).
        
        Args:
            func_type_prefix: "floc" or "ret" (for ret, will match ret_retRW, ret_retCB, etc.)
            min_runs: Minimum number of runs required
            expected_files: Expected number of files per run
        """
        issues = []
        
        # Get all functional series (for ret, this includes all ret tasks)
        if func_type_prefix == "ret":
            func_series = [s for s in self.series if s.get_series_type()[0].startswith("ret_")]
        else:
            func_series = [s for s in self.series if s.get_series_type()[0] == func_type_prefix]
        
        # Group by main type (to separate different ret tasks)
        by_main_type = defaultdict(list)
        for s in func_series:
            main_type, _ = s.get_series_type()
            by_main_type[main_type].append(s)
        
        # Check each main type separately
        for main_type, type_series in by_main_type.items():
            # Extract task name for display (e.g., "retRW" from "ret_retrw")
            if main_type.startswith("ret_"):
                task_name = main_type.split("_")[1].upper()
                display_name = task_name
            else:
                display_name = func_type_prefix
            
            # Group by subtype
            by_subtype = defaultdict(list)
            for s in type_series:
                _, subtype = s.get_series_type()
                by_subtype[subtype].append(s)
            
            # Count valid runs (mag series with correct file count)
            valid_mag_runs = [s for s in by_subtype.get("mag", []) if s.series_files == expected_files]
            num_valid_mag = len(valid_mag_runs)
            
            # Total mag runs (including those that might be reruns)
            total_mag_runs = len(by_subtype.get("mag", []))
            
            # For phase and SBRef, check if there's a valid version for each mag run
            num_pha_runs = len(by_subtype.get("Pha", []))
            valid_pha_runs = [s for s in by_subtype.get("Pha", []) if s.series_files == expected_files]
            num_valid_pha = len(valid_pha_runs)
            
            num_sbref_mag = len(by_subtype.get("SBRef_mag", []))
            valid_sbref_mag = [s for s in by_subtype.get("SBRef_mag", []) if s.series_files == 1]
            num_valid_sbref_mag = len(valid_sbref_mag)
            
            num_sbref_pha = len(by_subtype.get("SBRef_Pha", []))
            valid_sbref_pha = [s for s in by_subtype.get("SBRef_Pha", []) if s.series_files == 1]
            num_valid_sbref_pha = len(valid_sbref_pha)
            
            # Check minimum runs (use valid runs)
            if num_valid_mag < min_runs:
                issues.append(f"{display_name}: Only {num_valid_mag} valid runs (expected at least {min_runs})")
            
            # Check that valid counts match
            if num_valid_mag != num_valid_pha:
                issues.append(f"{display_name}: Valid mag runs ({num_valid_mag}) ≠ valid Pha runs ({num_valid_pha})")
            
            # For SBRef, we need AT LEAST as many as valid mag runs (extras are okay - they're from reruns)
            if num_valid_sbref_mag < num_valid_mag:
                issues.append(f"{display_name}: Valid SBRef_mag ({num_valid_sbref_mag}) < valid mag runs ({num_valid_mag})")
            
            if num_valid_sbref_pha < num_valid_mag:
                issues.append(f"{display_name}: Valid SBRef_Pha ({num_valid_sbref_pha}) < valid mag runs ({num_valid_mag})")
            
            # Report runs with incorrect file counts (that weren't successfully rerun)
            for i, series in enumerate(by_subtype.get("mag", []), 1):
                if series.series_files != expected_files:
                    # Check if there's a successful rerun
                    series_type = series.get_series_type()
                    if not self.has_successful_rerun(series_type, expected_files):
                        issues.append(f"{display_name} run-{i:02d} mag: Has {series.series_files} files (expected {expected_files}, no successful rerun)")
            
            for i, series in enumerate(by_subtype.get("Pha", []), 1):
                if series.series_files != expected_files:
                    series_type = series.get_series_type()
                    if not self.has_successful_rerun(series_type, expected_files):
                        issues.append(f"{display_name} run-{i:02d} Pha: Has {series.series_files} files (expected {expected_files}, no successful rerun)")
            
            # We don't check individual SBRef file counts here anymore since we only care about
            # having enough valid ones total (extras from reruns are fine)
        
        return issues
    
    def get_issues(self) -> List[str]:
        """Get list of all issues for this session."""
        issues = []
        
        issues.extend(self.check_t1_mp2rage())
        issues.extend(self.check_t2())
        issues.extend(self.check_dwi())
        issues.extend(self.check_functional("floc", min_runs=10, expected_files=160))
        issues.extend(self.check_functional("ret", min_runs=1, expected_files=156))  # At least 1 run for ret
        
        return issues
    
    @property
    def is_complete(self) -> bool:
        """Check if session is complete (no issues)."""
        return len(self.get_issues()) == 0
    
    @property
    def has_t1(self) -> bool:
        """Check if session has T1."""
        return any(s.get_series_type()[0] == "t1" for s in self.series)
    
    @property
    def has_t2(self) -> bool:
        """Check if session has T2."""
        return any(s.get_series_type()[0] == "t2" for s in self.series)
    
    @property
    def has_dwi(self) -> bool:
        """Check if session has DWI."""
        return any(s.get_series_type()[0] == "dwi" for s in self.series)
    
    @property
    def has_floc(self) -> bool:
        """Check if session has fLoc."""
        return any(s.get_series_type()[0] == "floc" for s in self.series)
    
    @property
    def has_ret(self) -> bool:
        """Check if session has ret."""
        return any(s.get_series_type()[0].startswith("ret_") for s in self.series)
    
    @property
    def num_floc_runs(self) -> int:
        """Count valid fLoc runs (mag with 160 files)."""
        return sum(1 for s in self.series 
                  if s.get_series_type() == ("floc", "mag") and s.series_files == 160)
    
    @property
    def ret_tasks(self) -> Dict[str, int]:
        """
        Get retinotopy tasks and their valid run counts.
        
        Returns:
            Dict mapping task name (e.g., "retRW") to number of valid runs
        """
        tasks = defaultdict(int)
        for s in self.series:
            main_type, subtype = s.get_series_type()
            if main_type.startswith("ret_") and subtype == "mag" and s.series_files == 156:
                task_name = main_type.split("_")[1].upper()
                tasks[task_name] += 1
        return dict(tasks)
    
    @property
    def num_ret_runs(self) -> int:
        """Count total valid ret runs across all tasks."""
        return sum(self.ret_tasks.values())


def parse_dicominfo_file(info_file: Path) -> List[SeriesInfo]:
    """
    Parse a dicominfo.tsv file.
    
    Expected columns: total_files_till_now, example_dcm_file, series_id, dcm_dir_name, ...
    """
    series_list = []
    
    try:
        with open(info_file, 'r') as f:
            # Use tab delimiter for TSV
            reader = csv.DictReader(f, delimiter='\t')
            
            for row in reader:
                # Get required fields
                series_id = row.get('series_id', '')
                dcm_dir_name = row.get('uniq_file_template', row.get('dcm_dir_name', ''))
                
                # For series_files, look in multiple possible columns
                series_files_str = row.get('series_files', 
                                          row.get('num_files',
                                          row.get('total_files_till_now', '0')))
                
                series_description = row.get('series_description', '')
                
                # Convert series_files to int
                try:
                    series_files = int(series_files_str)
                except (ValueError, TypeError):
                    series_files = 0
                
                if dcm_dir_name:  # Only add if we have a directory name
                    series_list.append(SeriesInfo(
                        series_id=str(series_id),
                        dcm_dir_name=dcm_dir_name,
                        series_files=series_files,
                        series_description=series_description
                    ))
    
    except Exception as e:
        console.print(f"[yellow]Warning: Could not parse {info_file.name}: {e}[/yellow]")
    
    return series_list


def find_info_files(heudiconv_dir: Path) -> List[tuple]:
    """
    Find all dicominfo files in .heudiconv directory.
    
    Expected structure: .heudiconv/sub-id/ses-xx/info/dicominfo_ses-xx.tsv
    
    Note: Ignores .heudiconv/sub-id/info/ (from step 1)
    
    Returns:
        List of (subject, session, info_file_path) tuples
    """
    info_files = []
    
    # Look for subject directories (numeric: 01, 02, 03, etc.)
    for sub_dir in sorted(heudiconv_dir.iterdir()):
        if not sub_dir.is_dir():
            continue
        if sub_dir.name == "info":
            continue  # Skip top-level info directory
        subject = sub_dir.name  # e.g., "01", "05"
        
        # Look for session directories (ses-XX or ses-XXrerun)
        for ses_dir in sorted(sub_dir.iterdir()):
            if not ses_dir.is_dir():
                continue
            
            session_name = ses_dir.name
            
            # Skip the "info" directory at subject level (from heudiconv step 1)
            if session_name == "info":
                continue
            
            # Only process session directories (ses-XX format)
            if not session_name.startswith("ses-"):
                continue
            
            # Look for info subdirectory
            info_dir = ses_dir / "info"
            if not info_dir.exists():
                continue
            
            # Look for dicominfo*.tsv files
            for info_file in info_dir.glob("dicominfo*.tsv"):
                info_files.append((subject, session_name, info_file))
    
    return info_files


def check_session(subject: str, session: str, info_file: Path) -> SessionCheck:
    """Check a single session's dicominfo file."""
    file_exists = info_file.exists()
    
    if file_exists:
        series = parse_dicominfo_file(info_file)
    else:
        series = []
    
    return SessionCheck(
        subject=subject,
        session=session,
        info_file=info_file,
        file_exists=file_exists,
        series=series
    )


def check_all_sessions(heudiconv_dir: Path) -> List[SessionCheck]:
    """Check all sessions in the heudiconv directory."""
    info_files = find_info_files(heudiconv_dir)
    
    if not info_files:
        console.print(f"[yellow]No dicominfo files found in {heudiconv_dir}[/yellow]")
        console.print(f"[yellow]Expected structure: .heudiconv/sub-id/ses-xx/info/dicominfo*.tsv[/yellow]")
        return []
    
    results = []
    with typer.progressbar(
        info_files,
        label="Checking sessions",
        show_pos=True,
        length=len(info_files)
    ) as progress:
        for subject, session, info_file in progress:
            results.append(check_session(subject, session, info_file))
    
    return results


def print_summary(results: List[SessionCheck]):
    """Print summary statistics."""
    total = len(results)
    complete = sum(1 for r in results if r.is_complete)
    incomplete = total - complete
    
    console.print(f"\n[bold]Summary:[/bold]")
    console.print(f"  Total sessions: {total}")
    console.print(f"  [green]✓ Complete sessions: {complete}[/green]")
    console.print(f"  [red]✗ Incomplete sessions: {incomplete}[/red]")


def print_scan_summary(results: List[SessionCheck]):
    """Print summary of scan types."""
    console.print(f"\n[bold]Scan Type Summary:[/bold]")
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Scan Type", style="cyan")
    table.add_column("Sessions", style="green")
    table.add_column("Coverage", style="blue")
    
    total = len(results)
    
    scan_types = [
        ("T1 MP2RAGE", sum(1 for r in results if r.has_t1)),
        ("T2", sum(1 for r in results if r.has_t2)),
        ("DWI", sum(1 for r in results if r.has_dwi)),
        ("fLoc", sum(1 for r in results if r.has_floc)),
        ("Retinotopy", sum(1 for r in results if r.has_ret)),
    ]
    
    for scan_name, count in scan_types:
        coverage = f"{count}/{total} ({100*count/total:.0f}%)" if total > 0 else "N/A"
        table.add_row(scan_name, str(count), coverage)
    
    console.print(table)
    
    # Print run statistics for functional scans
    if any(r.has_floc for r in results):
        avg_floc = sum(r.num_floc_runs for r in results if r.has_floc) / sum(1 for r in results if r.has_floc)
        console.print(f"\n  fLoc: Average {avg_floc:.1f} valid runs per session")
    
    if any(r.has_ret for r in results):
        # Collect all ret tasks
        all_tasks = set()
        for r in results:
            all_tasks.update(r.ret_tasks.keys())
        
        console.print(f"\n  Retinotopy tasks found: {', '.join(sorted(all_tasks))}")
        
        # Per-task statistics
        for task in sorted(all_tasks):
            sessions_with_task = [r for r in results if task in r.ret_tasks]
            if sessions_with_task:
                avg_runs = sum(r.ret_tasks[task] for r in sessions_with_task) / len(sessions_with_task)
                console.print(f"    {task}: {len(sessions_with_task)} sessions, average {avg_runs:.1f} valid runs")


def print_detailed_issues(results: List[SessionCheck]):
    """Print detailed issues for incomplete sessions."""
    incomplete = [r for r in results if not r.is_complete]
    
    if not incomplete:
        console.print("\n[bold green]✓ All sessions complete![/bold green]")
        return
    
    console.print(f"\n[bold red]Incomplete Sessions ({len(incomplete)}):[/bold red]\n")
    
    # Group by issue category
    by_category = defaultdict(list)
    for result in incomplete:
        issues = result.get_issues()
        for issue in issues:
            # Extract category (first part before colon)
            category = issue.split(':')[0] if ':' in issue else "Other"
            by_category[category].append((result, issue))
    
    # Print by category
    for category in sorted(by_category.keys()):
        items = by_category[category]
        # Get unique sessions for this category
        unique_sessions = set(item[0].session_id for item in items)
        
        console.print(f"[yellow]{category} issues ({len(unique_sessions)} sessions):[/yellow]")
        
        # Group by session
        by_session = defaultdict(list)
        for result, issue in items:
            by_session[result.session_id].append(issue)
        
        for session_id in sorted(by_session.keys()):
            console.print(f"  {session_id}:")
            for issue in by_session[session_id]:
                console.print(f"    - {issue}")
        console.print()


def print_session_details(results: List[SessionCheck], show_complete: bool = False):
    """Print detailed information for each session."""
    sessions_to_show = results if show_complete else [r for r in results if not r.is_complete]
    
    if not sessions_to_show:
        return
    
    console.print(f"\n[bold]Session Details:[/bold]\n")
    
    for result in sorted(sessions_to_show, key=lambda x: x.session_id):
        status = "[green]✓[/green]" if result.is_complete else "[red]✗[/red]"
        console.print(f"{status} [cyan]{result.session_id}[/cyan]")
        console.print(f"  Info file: {result.info_file.relative_to(result.info_file.parent.parent.parent)}")
        console.print(f"  Total series: {len(result.series)}")
        
        # Format ret tasks
        ret_tasks_str = ", ".join(f"{task}:{count}" for task, count in sorted(result.ret_tasks.items()))
        console.print(f"  Scans: T1={result.has_t1}, T2={result.has_t2}, DWI={result.has_dwi}, "
                     f"fLoc={result.num_floc_runs} runs, ret=[{ret_tasks_str}]")
        
        issues = result.get_issues()
        if issues:
            console.print(f"  [yellow]Issues ({len(issues)}):[/yellow]")
            for issue in issues[:10]:  # Show first 10 issues
                console.print(f"    - {issue}")
            if len(issues) > 10:
                console.print(f"    ... and {len(issues) - 10} more issues")
        
        console.print()


def export_report(results: List[SessionCheck], output_file: Path):
    """Export detailed report to TSV file."""
    with open(output_file, 'w') as f:
        f.write("subject\tsession\thas_T1\thas_T2\thas_DWI\tnum_floc_runs\tret_tasks\tnum_ret_runs\t"
               "num_series\tnum_issues\tissues\tstatus\n")
        
        for result in sorted(results, key=lambda x: x.session_id):
            issues = result.get_issues()
            issues_str = "; ".join(issues)
            status = "complete" if result.is_complete else "incomplete"
            
            # Format ret tasks
            ret_tasks_str = ",".join(f"{task}:{count}" for task, count in sorted(result.ret_tasks.items()))
            
            f.write(f"{result.subject}\t{result.session}\t"
                   f"{result.has_t1}\t{result.has_t2}\t{result.has_dwi}\t"
                   f"{result.num_floc_runs}\t{ret_tasks_str}\t{result.num_ret_runs}\t"
                   f"{len(result.series)}\t{len(issues)}\t{issues_str}\t{status}\n")
    
    console.print(f"\n[green]Report exported to: {output_file}[/green]")


@app.command()
def main(
    heudiconv_dir: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=False,
        dir_okay=True,
        help="Path to .heudiconv directory"
    ),
    show_complete: bool = typer.Option(
        False,
        "--show-complete",
        "-c",
        help="Show details for complete sessions too"
    ),
    output_file: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Export detailed report to TSV file"
    )
):
    """
    Check HeuDiConv DICOM info files for completeness.
    
    This script analyzes dicominfo*.tsv files in .heudiconv/sub-id/ses-xx/info/ and checks:
    
    T1 MP2RAGE: 3 series (INV1, INV2, UNI) - 1 file each
    T2: 1 series - 1 file
    DWI:
      - b06_PA: mag (6 files), Pha (6 files), SBRef_mag (1 file), SBRef_Pha (1 file)
      - dir104_AP: mag (105 files), Pha (105 files), SBRef_mag (1 file), SBRef_Pha (1 file)
    fLoc: At least 10 runs, each with:
      - mag (160 files), Pha (160 files), SBRef_mag (1 file), SBRef_Pha (1 file)
    Retinotopy: At least 1 run per task (retRW, retCB, retFF, etc.), each with:
      - mag (156 files), Pha (156 files), SBRef_mag (1 file), SBRef_Pha (1 file)
    
    The script handles reruns: if a series has incorrect file count but a subsequent
    series with the same name has the correct count, it's not marked as an error.
    
    Expected directory structure:
      .heudiconv/sub-id/ses-xx/info/dicominfo*.tsv
    
    Example:
        check_heudiconv_info.py /path/to/.heudiconv
        check_heudiconv_info.py /path/to/.heudiconv --output report.tsv
        check_heudiconv_info.py /path/to/.heudiconv --show-complete
    """
    console.print(f"[bold]Checking HeuDiConv DICOM info files[/bold]")
    console.print(f"  HeuDiConv dir: {heudiconv_dir}\n")
    
    results = check_all_sessions(heudiconv_dir)
    
    if not results:
        console.print("[yellow]No sessions found to check.[/yellow]")
        return
    
    print_summary(results)
    print_scan_summary(results)
    print_detailed_issues(results)
    
    if show_complete or any(not r.is_complete for r in results):
        print_session_details(results, show_complete)
    
    if output_file:
        export_report(results, output_file)


if __name__ == "__main__":
    app()
