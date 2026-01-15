#!/usr/bin/env python3
"""
Check PRF analysis outputs for completeness.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer()
console = Console()


# Expected file suffixes for each hemisphere
EXPECTED_SUFFIXES = [
    'centerx0.nii.gz',
    'centery0.nii.gz',
    'estimates.json',
    'modelpred.nii.gz',
    'r2.nii.gz',
    'results.mat',
    'sigmamajor.nii.gz',
    'sigmaminor.nii.gz',
    'testdata.nii.gz',
    'theta.nii.gz',
]


@dataclass
class RunResult:
    """Results for a single run."""
    run_id: str
    hemi_l_files: set[str]
    hemi_r_files: set[str]

    @property
    def missing_hemi_l(self) -> list[str]:
        """Get missing suffixes for hemi-L."""
        found_suffixes = {f.split('_hemi-L_')[1] for f in self.hemi_l_files if '_hemi-L_' in f}
        return [s for s in EXPECTED_SUFFIXES if s not in found_suffixes]

    @property
    def missing_hemi_r(self) -> list[str]:
        """Get missing suffixes for hemi-R."""
        found_suffixes = {f.split('_hemi-R_')[1] for f in self.hemi_r_files if '_hemi-R_' in f}
        return [s for s in EXPECTED_SUFFIXES if s not in found_suffixes]

    @property
    def is_complete(self) -> bool:
        """Check if this run has all required files."""
        return len(self.missing_hemi_l) == 0 and len(self.missing_hemi_r) == 0


@dataclass
class SessionResult:
    """Results for a single subject/session."""
    subject: str
    session: str
    folder_exists: bool
    runs: dict[str, RunResult]  # run_id -> RunResult

    @property
    def num_runs(self) -> int:
        """Number of runs found."""
        return len(self.runs)

    @property
    def complete_runs(self) -> int:
        """Number of complete runs."""
        return sum(1 for r in self.runs.values() if r.is_complete)

    @property
    def is_complete(self) -> bool:
        """Check if all runs are complete."""
        return self.folder_exists and all(r.is_complete for r in self.runs.values())


def extract_run_prefix(filename: str) -> str:
    """
    Extract the run prefix from a filename.

    Example: sub-01_ses-02_task-retRW_run-01_hemi-L_r2.nii.gz
             -> sub-01_ses-02_task-retRW_run-01
    """
    if '_hemi-' in filename:
        return filename.split('_hemi-')[0]
    return ''


def get_expected_combinations() -> list[tuple[str, str]]:
    """Generate all expected sub/ses combinations, excluding sessions with no data."""
    # Sessions to exclude (no data or missing both tasks)
    excluded_sessions = {
        # No data
        ('sub-01', 'ses-08'),
        ('sub-03', 'ses-08'),
        ('sub-05', 'ses-07'),
        ('sub-05', 'ses-08'),
        ('sub-08', 'ses-04'),
        ('sub-10', 'ses-02'),
        ('sub-10', 'ses-03'),
        ('sub-10', 'ses-04'),
        ('sub-10', 'ses-06'),
        ('sub-11', 'ses-10'),
        # Missing both tasks
        ('sub-10', 'ses-01'),
        ('sub-10', 'ses-05'),
        ('sub-10', 'ses-07'),
        ('sub-10', 'ses-08'),
        ('sub-10', 'ses-09'),
        ('sub-10', 'ses-10'),
    }

    combinations = []
    for sub_id in range(1, 12):  # 01-11
        for ses_id in range(1, 11):  # 01-10
            sub_str = f'sub-{sub_id:02d}'
            ses_str = f'ses-{ses_id:02d}'

            # Skip excluded sessions
            if (sub_str, ses_str) not in excluded_sessions:
                combinations.append((sub_str, ses_str))

    return combinations


def check_prf_analysis(
    func_dir: Path,
    subject: str,
    session: str,
) -> dict[str, RunResult]:
    """
    Check PRF analysis files for a given subject/session.

    Returns:
        Dictionary mapping run_id to RunResult
    """
    if not func_dir.exists():
        return {}

    # Group files by run
    runs_data = defaultdict(lambda: {'hemi_l': set(), 'hemi_r': set()})

    for file_path in func_dir.iterdir():
        if not file_path.is_file():
            continue

        filename = file_path.name

        # Check if this is a PRF analysis file
        if '_hemi-L_' in filename:
            run_prefix = extract_run_prefix(filename)
            if run_prefix:
                runs_data[run_prefix]['hemi_l'].add(filename)
        elif '_hemi-R_' in filename:
            run_prefix = extract_run_prefix(filename)
            if run_prefix:
                runs_data[run_prefix]['hemi_r'].add(filename)

    # Convert to RunResult objects
    results = {}
    for run_prefix, data in runs_data.items():
        # Extract just the run ID for cleaner display
        run_id = run_prefix.split('_')[-1] if '_' in run_prefix else run_prefix
        results[run_id] = RunResult(
            run_id=run_id,
            hemi_l_files=data['hemi_l'],
            hemi_r_files=data['hemi_r'],
        )

    return results


def check_all_sessions(analysis_dir: Path) -> list[SessionResult]:
    """Check PRF analysis outputs across all subjects/sessions."""
    expected = get_expected_combinations()
    results = []

    with typer.progressbar(
        expected,
        label='Checking PRF analysis results',
        show_pos=True,
        length=len(expected),
    ) as progress:
        for sub, ses in progress:
            func_dir = analysis_dir / sub / ses
            folder_exists = func_dir.exists()

            if folder_exists:
                runs = check_prf_analysis(func_dir, sub, ses)
            else:
                runs = {}

            results.append(
                SessionResult(
                    subject=sub,
                    session=ses,
                    folder_exists=folder_exists,
                    runs=runs,
                ),
            )

    return results


def print_summary(results: list[SessionResult]):
    """Print summary statistics."""
    total_sessions = len(results)
    sessions_with_data = sum(1 for r in results if r.num_runs > 0)
    complete_sessions = sum(1 for r in results if r.is_complete and r.num_runs > 0)
    missing_folders = sum(1 for r in results if not r.folder_exists)
    no_runs = sum(1 for r in results if r.folder_exists and r.num_runs == 0)
    incomplete_sessions = sum(1 for r in results if r.num_runs > 0 and not r.is_complete)

    total_runs = sum(r.num_runs for r in results)
    complete_runs = sum(r.complete_runs for r in results)

    console.print('\n[bold]Summary:[/bold]')
    console.print(f'  Total sessions: {total_sessions}')
    console.print(
        f'  [green]✓ Complete sessions: {complete_sessions}/{sessions_with_data}[/green]')
    console.print(f'  [red]✗ Incomplete sessions: {incomplete_sessions}[/red]')

    console.print('\n[bold]Runs:[/bold]')
    console.print(f'  Total runs found: {total_runs}')
    console.print(f'  [green]✓ Complete runs: {complete_runs}[/green]')
    console.print(f'  [red]✗ Incomplete runs: {total_runs - complete_runs}[/red]')

    if missing_folders > 0 or no_runs > 0:
        console.print('\n[bold]Issues:[/bold]')
        if missing_folders > 0:
            console.print(f'  [yellow]Missing folders: {missing_folders}[/yellow]')
        if no_runs > 0:
            console.print(f'  [yellow]No runs found: {no_runs}[/yellow]')


def print_detailed_results(results: list[SessionResult], verbose: bool = False):
    """Print detailed results for incomplete sessions."""
    incomplete_results = [r for r in results if not r.is_complete or r.num_runs == 0]

    if not incomplete_results:
        console.print('\n[bold green]✓ All sessions complete![/bold green]')
        return

    console.print(f'\n[bold red]Incomplete Sessions ({len(incomplete_results)}):[/bold red]\n')

    # Group by issue type
    missing_folders = [r for r in incomplete_results if not r.folder_exists]
    no_runs = [r for r in incomplete_results if r.folder_exists and r.num_runs == 0]
    incomplete_runs = [r for r in incomplete_results if r.num_runs > 0 and not r.is_complete]

    if missing_folders:
        console.print(f'[bold yellow]Missing Folders ({len(missing_folders)}):[/bold yellow]')
        for result in missing_folders:
            console.print(f'  {result.subject}/{result.session}')
        console.print()

    if no_runs:
        console.print(f'[bold yellow]No Runs Found ({len(no_runs)}):[/bold yellow]')
        for result in no_runs:
            console.print(f'  {result.subject}/{result.session}')
        console.print()

    if incomplete_runs:
        console.print(f'[bold yellow]Incomplete Runs ({len(incomplete_runs)}):[/bold yellow]\n')

        for result in incomplete_runs:
            console.print(
                f'[cyan]{result.subject}/{result.session}[/cyan] '
                f'({result.complete_runs}/{result.num_runs} complete runs)',
            )

            for run_id, run_result in sorted(result.runs.items()):
                if run_result.is_complete:
                    if verbose:
                        console.print(f'  [green]✓ {run_id}[/green]')
                    continue

                console.print(f'  [red]✗ {run_id}[/red]')

                missing_l = run_result.missing_hemi_l
                missing_r = run_result.missing_hemi_r

                if missing_l:
                    console.print(f'    [yellow]Missing hemi-L ({len(missing_l)}):[/yellow]')
                    for suffix in missing_l:
                        console.print(f'      - {suffix}')

                if missing_r:
                    console.print(f'    [yellow]Missing hemi-R ({len(missing_r)}):[/yellow]')
                    for suffix in missing_r:
                        console.print(f'      - {suffix}')

            console.print()


def print_run_distribution(results: list[SessionResult]):
    """Print distribution of runs per session."""
    run_counts = defaultdict(int)
    for result in results:
        if result.num_runs > 0:
            run_counts[result.num_runs] += 1

    if not run_counts:
        return

    console.print('[bold]Run Distribution:[/bold]')
    table = Table(show_header=True, header_style='bold magenta')
    table.add_column('Number of Runs', style='cyan')
    table.add_column('Number of Sessions', style='green')

    for num_runs in sorted(run_counts.keys()):
        table.add_row(str(num_runs), str(run_counts[num_runs]))

    console.print(table)
    console.print()


@app.command()
def main(
    analysis_dir: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=False,
        dir_okay=True,
        help='Path to analysis directory containing sub-XX/ses-XX/func folders',
    ),
    verbose: bool = typer.Option(
        False,
        '--verbose',
        '-v',
        help='Show complete runs in addition to incomplete ones',
    ),
    show_distribution: bool = typer.Option(
        True,
        '--show-distribution/--no-distribution',
        help='Show distribution of runs per session',
    ),
):
    """
    Check PRF analysis outputs for completeness.

    For each subject/session, this script:
    1. Discovers all unique runs (by checking unique prefixes before _hemi-)
    2. Verifies each run has all 10 expected files for both hemispheres
    3. Reports missing files by run and hemisphere

    Expected files per hemisphere per run:
    - centerx0.nii.gz, centery0.nii.gz
    - estimates.json, modelpred.nii.gz
    - r2.nii.gz, results.mat
    - sigmamajor.nii.gz, sigmaminor.nii.gz
    - testdata.nii.gz, theta.nii.gz

    Example:
        check_prf_analysis.py /path/to/analysis
        check_prf_analysis.py /path/to/analysis --verbose
    """
    console.print('[bold]Checking PRF analysis results[/bold]')
    console.print(f'  Analysis dir: {analysis_dir}')
    console.print(f'  Expected files per hemi per run: {len(EXPECTED_SUFFIXES)}\n')

    results = check_all_sessions(analysis_dir)

    print_summary(results)

    if show_distribution:
        console.print()
        print_run_distribution(results)

    print_detailed_results(results, verbose)


if __name__ == '__main__':
    app()
