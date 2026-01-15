#!/usr/bin/env python3
"""
Check PRF preparation outputs for completeness.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer()
console = Console()


@dataclass
class PRFCheckResult:
    """Results for a single subject/session."""
    subject: str
    session: str
    folder_exists: bool
    hemi_l_files: list[str]
    hemi_r_files: list[str]

    @property
    def is_complete(self) -> bool:
        """Check if this sub/ses has all required files."""
        return (
            self.folder_exists
            and len(self.hemi_l_files) == len(self.hemi_r_files)
            and len(self.hemi_l_files) > 0
        )

    @property
    def status(self) -> str:
        """Get status string."""
        if not self.folder_exists:
            return 'MISSING_FOLDER'
        if not self.hemi_l_files and not self.hemi_r_files:
            return 'NO_FILES'
        if len(self.hemi_l_files) != len(self.hemi_r_files):
            return 'INCOMPLETE_HEMIS'
        return 'COMPLETE'


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


def check_prf_files(
    func_dir: Path,
    subject: str,
    session: str,
    annot_pattern: str,
    files_per_hemi: int,
) -> tuple[list[str], list[str]]:
    """
    Check for PRF files matching the pattern.

    Expected pattern: {sub}_{ses}_hemi-{L/R}_{annot_pattern}*

    Returns:
        Tuple of (hemi_l_files, hemi_r_files)
    """
    if not func_dir.exists():
        return [], []

    # Look for files matching the pattern
    hemi_l_files = []
    hemi_r_files = []

    # Pattern: sub-XX_ses-XX_hemi-L_*{annot_pattern}*
    for hemi, file_list in [('L', hemi_l_files), ('R', hemi_r_files)]:
        pattern = f'{subject}_{session}_hemi-{hemi}_*{annot_pattern}*'
        matching_files = list(func_dir.glob(pattern))
        file_list.extend([f.name for f in matching_files])

    return hemi_l_files, hemi_r_files


def check_analysis_folder(
    analysis_dir: Path,
    annot_pattern: str,
    files_per_hemi: int,
) -> list[PRFCheckResult]:
    """
    Check PRF preparation outputs across all subjects/sessions.

    Args:
        analysis_dir: Root analysis directory
        annot_pattern: Pattern to match in filenames (e.g., "prf_VOTC")
        files_per_hemi: Expected number of files per hemisphere

    Returns:
        List of PRFCheckResult objects
    """
    expected = get_expected_combinations()
    results = []

    with typer.progressbar(
        expected,
        label=f'Checking PRF files ({annot_pattern})',
        show_pos=True,
        length=len(expected),
    ) as progress:
        for sub, ses in progress:
            func_dir = analysis_dir / sub / ses / 'func'
            folder_exists = func_dir.exists()

            if folder_exists:
                hemi_l, hemi_r = check_prf_files(
                    func_dir, sub, ses, annot_pattern, files_per_hemi,
                )
            else:
                hemi_l, hemi_r = [], []

            results.append(
                PRFCheckResult(
                    subject=sub,
                    session=ses,
                    folder_exists=folder_exists,
                    hemi_l_files=hemi_l,
                    hemi_r_files=hemi_r,
                ),
            )

    return results


def print_summary(results: list[PRFCheckResult], files_per_hemi: int):
    """Print summary statistics with breakdown by issue type."""
    complete = sum(
        1 for r in results if r.is_complete
        and len(r.hemi_l_files) == files_per_hemi
    )

    # Count specific issue types
    missing_folders = sum(1 for r in results if not r.folder_exists)
    no_files = sum(
        1 for r in results if r.folder_exists
        and not r.hemi_l_files and not r.hemi_r_files
    )
    incomplete_hemis = sum(
        1 for r in results if r.folder_exists
        and (r.hemi_l_files or r.hemi_r_files)
        and len(r.hemi_l_files) != len(r.hemi_r_files)
    )
    wrong_count = sum(
        1 for r in results if r.folder_exists
        and len(r.hemi_l_files) == len(r.hemi_r_files)
        and len(r.hemi_l_files) > 0
        and len(r.hemi_l_files) != files_per_hemi
    )

    total_incomplete = missing_folders + no_files + incomplete_hemis + wrong_count

    console.print('\n[bold]Summary:[/bold]')
    console.print(f'  Total combinations: {len(results)}')
    console.print(f'  [green]✓ Complete ({files_per_hemi} files/hemi): {complete}[/green]')
    console.print(f'  [red]✗ Incomplete or missing: {total_incomplete}[/red]')

    if total_incomplete > 0:
        console.print('\n[bold]Breakdown of issues:[/bold]')
        if missing_folders > 0:
            console.print(f'  [yellow]Missing folders: {missing_folders}[/yellow]')
        if no_files > 0:
            console.print(f'  [yellow]No files found: {no_files}[/yellow]')
        if incomplete_hemis > 0:
            console.print(f'  [yellow]Incomplete hemispheres (L≠R): {incomplete_hemis}[/yellow]')
        if wrong_count > 0:
            console.print(
                f'  [yellow]Wrong file count (L=R but ≠{files_per_hemi}): {wrong_count}[/yellow]')


def print_detailed_results(results: list[PRFCheckResult], files_per_hemi: int):
    """Print detailed results for incomplete combinations."""
    incomplete_results = [
        r for r in results
        if not r.is_complete or len(r.hemi_l_files) != files_per_hemi
    ]

    if not incomplete_results:
        console.print('\n[bold green]✓ All combinations complete![/bold green]')
        return

    console.print(f'\n[bold red]Detailed Issues ({len(incomplete_results)}):[/bold red]\n')

    # Group by status
    by_status: dict[str, list[PRFCheckResult]] = {}
    for result in incomplete_results:
        status = result.status
        # Add subcategory for complete but wrong count
        if status == 'COMPLETE' and len(result.hemi_l_files) != files_per_hemi:
            status = 'WRONG_FILE_COUNT'

        if status not in by_status:
            by_status[status] = []
        by_status[status].append(result)

    # Define display order
    status_order = ['MISSING_FOLDER', 'NO_FILES', 'INCOMPLETE_HEMIS', 'WRONG_FILE_COUNT']
    status_labels = {
        'MISSING_FOLDER': 'Missing Folders',
        'NO_FILES': 'No Files Found',
        'INCOMPLETE_HEMIS': 'Incomplete Hemispheres (L≠R)',
        'WRONG_FILE_COUNT': f'Wrong File Count (L=R but ≠{files_per_hemi})',
    }

    for status in status_order:
        if status not in by_status:
            continue

        items = by_status[status]
        label = status_labels.get(status, status)
        console.print(f'[bold yellow]{label} ({len(items)}):[/bold yellow]')

        table = Table(show_header=True, header_style='bold magenta')
        table.add_column('Subject/Session', style='cyan')
        table.add_column('Hemi-L Files', style='green')
        table.add_column('Hemi-R Files', style='green')
        table.add_column('Expected', style='yellow')

        for result in items:
            table.add_row(
                f'{result.subject}/{result.session}',
                str(len(result.hemi_l_files)),
                str(len(result.hemi_r_files)),
                str(files_per_hemi),
            )

        console.print(table)
        console.print()


def print_file_details(results: list[PRFCheckResult], show_all: bool):
    """Print detailed file listings."""
    items_to_show = results if show_all else [
        r for r in results if not r.is_complete
    ]

    if not items_to_show:
        return

    console.print('[bold]File Details:[/bold]\n')

    for result in items_to_show:
        if not result.folder_exists:
            console.print(f'[red]{result.subject}/{result.session}: Folder not found[/red]')
            continue

        if not result.hemi_l_files and not result.hemi_r_files:
            console.print(f'[red]{result.subject}/{result.session}: No files found[/red]')
            continue

        console.print(f'[cyan]{result.subject}/{result.session}:[/cyan]')
        console.print(f'  Hemi-L ({len(result.hemi_l_files)}):')
        for f in result.hemi_l_files:
            console.print(f'    {f}')
        console.print(f'  Hemi-R ({len(result.hemi_r_files)}):')
        for f in result.hemi_r_files:
            console.print(f'    {f}')
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
    annot_pattern: str = typer.Option(
        'prf_VOTC',
        '--annot',
        '-a',
        help="Pattern to match in filenames (e.g., 'prf_VOTC')",
    ),
    files_per_hemi: int = typer.Option(
        3,
        '--files-per-hemi',
        '-n',
        help='Expected number of files per hemisphere',
    ),
    show_files: bool = typer.Option(
        False,
        '--show-files',
        '-f',
        help='Show detailed file listings for incomplete combinations',
    ),
    show_all_files: bool = typer.Option(
        False,
        '--show-all-files',
        help='Show file listings for ALL combinations',
    ),
):
    """
    Check PRF preparation outputs for completeness.

    Verifies that each sub-XX/ses-XX/func directory contains the expected
    number of files matching the annotation pattern for both hemispheres.

    Example:
        check_prf_prepare.py /analysis --annot prf_VOTC --files-per-hemi 3
    """
    console.print('[bold]Checking PRF preparation:[/bold]')
    console.print(f'  Analysis dir: {analysis_dir}')
    console.print(f'  Pattern: {annot_pattern}')
    console.print(f'  Expected files per hemisphere: {files_per_hemi}\n')

    results = check_analysis_folder(analysis_dir, annot_pattern, files_per_hemi)

    print_summary(results, files_per_hemi)
    print_detailed_results(results, files_per_hemi)

    if show_files or show_all_files:
        print_file_details(results, show_all_files)


if __name__ == '__main__':
    app()
