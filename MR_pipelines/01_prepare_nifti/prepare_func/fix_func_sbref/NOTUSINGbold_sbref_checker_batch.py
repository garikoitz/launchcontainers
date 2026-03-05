#!/usr/bin/env python3
"""
Batch Check and Rename SBRef Files
===================================

Check SBRef files across multiple subjects/sessions to ensure they match
their corresponding functional files by acquisition time.

Outputs same format as integrity checker:
1. sbref_check_incomplete.csv - Brief sub,ses,STATUS
2. sbref_check_detailed.log - Detailed log indexed [0], [1], ...
3. sbref_check_matrix.csv - Pivot table (subjects × sessions)
"""

import typer
import csv
from pathlib import Path
from datetime import datetime
import json
import pandas as pd
from collections import defaultdict
from dataclasses import dataclass, field
from rich.console import Console
from rich.table import Table
from rich.progress import track
from typing import List, Tuple, Optional

app = typer.Typer()
console = Console()

# Excluded sessions
EXCLUDED_SESSIONS = {
    # Add exclusions here, e.g.: ('sub-10', 'ses-02'),
}


@dataclass
class SBRefResult:
    """Result for a single session."""
    sub: str
    ses: str
    status: str  # 'ok', 'mismatch', 'missing', 'no_func'
    n_func: int = 0
    n_sbref: int = 0
    n_matched: int = 0
    n_need_rename: int = 0
    rename_plan: List[dict] = field(default_factory=list)
    issues: List[str] = field(default_factory=list)
    
    @property
    def is_complete(self) -> bool:
        """True if status is 'ok'."""
        return self.status == 'ok'


def default_combinations() -> List[Tuple[str, str]]:
    """All 11 subs × 10 sessions minus excluded."""
    combos = []
    for sub_id in range(1, 12):
        for ses_id in range(1, 11):
            sub = f"sub-{sub_id:02d}"
            ses = f"ses-{ses_id:02d}"
            if (sub, ses) not in EXCLUDED_SESSIONS:
                combos.append((sub, ses))
    return combos


def read_subseslist(filepath: Path) -> List[Tuple[str, str]]:
    """Read subject/session pairs from file."""
    combos = []
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split()
            if len(parts) >= 2:
                combos.append((parts[0], parts[1]))
    return combos


def get_acq_time(json_file: Path) -> Optional[datetime]:
    """Get acquisition time from JSON."""
    try:
        with open(json_file) as f:
            data = json.load(f)
        acq_time = data.get('AcquisitionTime')
        if acq_time:
            return datetime.strptime(acq_time.split('.')[0], '%H:%M:%S')
    except:
        pass
    return None


def check_session_sbrefs(
    bids_dir: Path,
    sub: str,
    ses: str,
    func_type: str,
    max_gap: int = 60,
) -> SBRefResult:
    """Check SBRef files for a single session."""
    func_dir = bids_dir / sub / ses / "func"
    
    result = SBRefResult(sub=sub, ses=ses, status='ok')
    
    if not func_dir.exists():
        result.status = 'no_func'
        result.issues.append('func directory does not exist')
        return result
    
    # Get files
    func_files = list(func_dir.glob(f"*_{func_type}.nii.gz"))
    sbref_files = list(func_dir.glob("*_sbref.nii.gz"))
    
    result.n_func = len(func_files)
    result.n_sbref = len(sbref_files)
    
    if not func_files:
        result.status = 'no_func'
        result.issues.append(f'No {func_type} files found')
        return result
    
    if not sbref_files:
        result.status = 'missing'
        result.issues.append('No sbref files found')
        return result
    
    # Match sbrefs to functional files
    dummy_date = datetime(2000, 1, 1)
    matched_count = 0
    need_rename_count = 0
    
    for sbref_nii in sbref_files:
        sbref_json = sbref_nii.with_suffix('').with_suffix('.json')
        sbref_time = get_acq_time(sbref_json)
        
        if not sbref_time:
            result.issues.append(f'No acquisition time: {sbref_nii.name}')
            continue
        
        # Find matching functional file by time
        best_match = None
        min_diff = 999999
        
        for func_nii in func_files:
            func_json = func_nii.with_suffix('').with_suffix('.json')
            func_time = get_acq_time(func_json)
            
            if not func_time:
                continue
            
            sbref_dt = datetime.combine(dummy_date, sbref_time.time())
            func_dt = datetime.combine(dummy_date, func_time.time())
            diff = abs((sbref_dt - func_dt).total_seconds())
            
            if diff < min_diff and diff <= max_gap:
                min_diff = diff
                best_match = func_nii
        
        if not best_match:
            result.issues.append(f'No time match for {sbref_nii.name}')
            continue
        
        matched_count += 1
        
        # Check if name matches
        func_base = best_match.stem.replace('.nii', '').replace(f'_{func_type}', '')
        expected_sbref_name = f"{func_base}_sbref.nii.gz"
        
        if sbref_nii.name != expected_sbref_name:
            need_rename_count += 1
            result.rename_plan.append({
                'old_name': sbref_nii.name,
                'new_name': expected_sbref_name,
                'matched_func': best_match.name,
                'time_diff': int(min_diff),
            })
    
    result.n_matched = matched_count
    result.n_need_rename = need_rename_count
    
    # Determine overall status
    if need_rename_count > 0:
        result.status = 'mismatch'
    elif matched_count != len(sbref_files):
        result.status = 'mismatch'
        result.issues.append(f'{len(sbref_files) - matched_count} sbrefs unmatched')
    elif matched_count != len(func_files):
        result.status = 'mismatch'
        result.issues.append(f'{len(func_files) - matched_count} funcs without sbref')
    else:
        result.status = 'ok'
    
    return result


# =============================================================================
# OUTPUT WRITERS (matching integrity checker format)
# =============================================================================

def write_brief_csv(results: List[SBRefResult], output_path: Path) -> None:
    """Write brief CSV: sub,ses,STATUS — row index matches detailed log."""
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["sub", "ses", "STATUS"])
        for r in results:
            writer.writerow([
                r.sub.replace("sub-", ""),
                r.ses.replace("ses-", ""),
                r.status,
            ])


def write_detailed_log(results: List[SBRefResult], output_path: Path, func_type: str) -> None:
    """Write detailed log indexed [0], [1], ... matching CSV rows."""
    with open(output_path, "w") as f:
        f.write(f"# SBREF CHECK — Detailed Log\n")
        f.write(f"# Functional type: {func_type}\n")
        f.write(f"# Generated: {datetime.now().isoformat()}\n")
        f.write(f"# {'=' * 70}\n\n")

        for idx, r in enumerate(results):
            sub_num = r.sub.replace("sub-", "")
            ses_num = r.ses.replace("ses-", "")

            f.write(f"[{idx}] sub={sub_num}, ses={ses_num} — {r.status.upper()}\n")

            if r.status == 'no_func':
                f.write("      func directory does not exist or no functional files.\n\n")
                continue

            f.write(
                f"      Functional: {r.n_func}  |  SBRef: {r.n_sbref}  |  "
                f"Matched: {r.n_matched}  |  Need rename: {r.n_need_rename}\n"
            )

            if r.issues:
                f.write("      Issues:\n")
                for issue in r.issues:
                    f.write(f"        - {issue}\n")

            if r.rename_plan:
                f.write("      Rename plan:\n")
                for item in r.rename_plan:
                    f.write(f"        {item['old_name']}\n")
                    f.write(f"        → {item['new_name']}\n")
                    f.write(f"          (matched with {item['matched_func']}, Δt={item['time_diff']}s)\n")

            f.write("\n")


def write_matrix_csv(results: List[SBRefResult], output_path: Path) -> None:
    """Write pivot table: subjects × sessions."""
    pivot_data = []
    for r in results:
        pivot_data.append({
            'sub': r.sub,
            'ses': r.ses,
            'status': r.status,
        })
    
    df = pd.DataFrame(pivot_data)
    pivot = df.pivot(index='sub', columns='ses', values='status')
    pivot.fillna('N/A', inplace=True)
    pivot.to_csv(output_path)


# =============================================================================
# CONSOLE OUTPUT
# =============================================================================

def print_summary(results: List[SBRefResult]) -> None:
    """Print summary statistics and table."""
    status_counts = defaultdict(int)
    for r in results:
        status_counts[r.status] += 1
    
    console.print("\n[bold]Status Summary:[/bold]")
    table = Table()
    table.add_column("Status", style="cyan")
    table.add_column("Count", justify="right", style="yellow")
    
    for status in ['ok', 'mismatch', 'missing', 'no_func']:
        count = status_counts[status]
        if count > 0:
            table.add_row(status.upper(), str(count))
    
    console.print(table)


def print_detailed_results(results: List[SBRefResult], show_ok: bool = False) -> None:
    """Print detailed results table."""
    # Filter results
    if not show_ok:
        results = [r for r in results if r.status != 'ok']
    
    if not results:
        console.print("\n[green]All sessions OK![/green]")
        return
    
    console.print(f"\n[bold]Detailed Results: {len(results)} sessions[/bold]")
    
    table = Table()
    table.add_column("sub", style="cyan")
    table.add_column("ses", style="cyan")
    table.add_column("Status", style="yellow")
    table.add_column("Func", justify="right")
    table.add_column("SBRef", justify="right")
    table.add_column("Matched", justify="right")
    table.add_column("Need Rename", justify="right")
    table.add_column("Issues", style="dim")
    
    for r in results:
        status_style = "green" if r.is_complete else "red"
        issues_str = "; ".join(r.issues[:2]) if r.issues else ""
        if len(r.issues) > 2:
            issues_str += "..."
        
        table.add_row(
            r.sub.replace("sub-", ""),
            r.ses.replace("ses-", ""),
            f"[{status_style}]{r.status}[/{status_style}]",
            str(r.n_func),
            str(r.n_sbref),
            str(r.n_matched),
            str(r.n_need_rename),
            issues_str,
        )
    
    console.print(table)


# =============================================================================
# MAIN COMMAND
# =============================================================================

@app.command()
def check(
    bids_dir: Path = typer.Option(..., "--bids", "-b", help="BIDS directory"),
    func_type: str = typer.Option(
        "bold",
        "--func-type",
        "-f",
        help="Functional file type: 'bold' or 'magnitude'",
    ),
    subseslist: Optional[Path] = typer.Option(
        None,
        "--subseslist",
        help="File with sub ses pairs (one per line)",
    ),
    output_dir: Path = typer.Option(
        ".",
        "--output",
        "-o",
        help="Output directory for reports",
    ),
    max_gap: int = typer.Option(
        60,
        "--max-gap",
        help="Max time gap (seconds) for matching",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show all sessions including OK",
    ),
):
    """
    Check SBRef files for multiple subjects/sessions.
    
    Outputs same format as analysis integrity checker.
    """
    console.print(f"[bold]Checking SBRef files[/bold]")
    console.print(f"BIDS dir: {bids_dir}")
    console.print(f"Functional type: {func_type}\n")
    
    # Get subject/session combinations
    if subseslist:
        combos = read_subseslist(subseslist)
        console.print(f"Loaded {len(combos)} combinations from {subseslist}")
    else:
        combos = default_combinations()
        console.print(f"Using default combinations: {len(combos)} sessions")
    
    # Check all sessions
    results = []
    for sub, ses in track(combos, description="Checking sessions..."):
        result = check_session_sbrefs(bids_dir, sub, ses, func_type, max_gap)
        results.append(result)
    
    # Print summary and detailed results
    print_summary(results)
    print_detailed_results(results, show_ok=verbose)
    
    # Write output files
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    brief_path = output_dir / f"sbref_check_incomplete.csv"
    detail_path = output_dir / f"sbref_check_detailed.log"
    matrix_path = output_dir / f"sbref_check_matrix.csv"
    
    write_brief_csv(results, brief_path)
    write_detailed_log(results, detail_path, func_type)
    write_matrix_csv(results, matrix_path)
    
    console.print(f"\n[bold]Brief CSV:[/bold]    {brief_path}")
    console.print(f"[bold]Detailed log:[/bold] {detail_path}")
    console.print(f"[bold]Matrix CSV:[/bold]   {matrix_path}")


if __name__ == "__main__":
    app()