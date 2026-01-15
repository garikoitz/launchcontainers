#!/usr/bin/env python3
"""
Check fMRIPrep preprocessing outputs for completeness.
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


# Expected file patterns for each run
EXPECTED_FILES = [
    'desc-brain_mask.nii.gz',
    'desc-confounds_timeseries.json',
    'desc-confounds_timeseries.tsv',
    'desc-coreg_boldref.json',
    'desc-coreg_boldref.nii.gz',
    'desc-hmc_boldref.json',
    'desc-hmc_boldref.nii.gz',
    'desc-preproc_bold.json',
    'desc-preproc_bold.nii.gz',
    # "from-boldref_to-T1w_mode-image_desc-coreg_xfm.json",
    # "from-boldref_to-T1w_mode-image_desc-coreg_xfm.txt",
    # "from-orig_to-boldref_mode-image_desc-hmc_xfm.json",
    # "from-orig_to-boldref_mode-image_desc-hmc_xfm.txt",
    'hemi-L_space-fsaverage_bold.func.gii',
    'hemi-L_space-fsaverage_bold.json',
    'hemi-L_space-fsnative_bold.func.gii',
    'hemi-L_space-fsnative_bold.json',
    'hemi-R_space-fsaverage_bold.func.gii',
    'hemi-R_space-fsaverage_bold.json',
    'hemi-R_space-fsnative_bold.func.gii',
    'hemi-R_space-fsnative_bold.json',
    # "space-MNI152NLin2009cAsym_boldref.json",
    # "space-MNI152NLin2009cAsym_boldref.nii.gz",
    # "space-MNI152NLin2009cAsym_desc-brain_mask.json",
    # "space-MNI152NLin2009cAsym_desc-brain_mask.nii.gz",
    # "space-MNI152NLin2009cAsym_desc-preproc_bold.json",
    # "space-MNI152NLin2009cAsym_desc-preproc_bold.nii.gz",
    # "space-T1w_boldref.json",
    # "space-T1w_boldref.nii.gz",
    # "space-T1w_desc-brain_mask.json",
    # "space-T1w_desc-brain_mask.nii.gz",
    # "space-T1w_desc-preproc_bold.json",
    # "space-T1w_desc-preproc_bold.nii.gz",
]


@dataclass
class RunResult:
    """Results for a single run."""
    run_id: str
    task: str
    found_files: set[str]

    @property
    def missing_files(self) -> list[str]:
        """Get missing file patterns (excluding optional files)."""
        # Extract suffixes from found files
        found_suffixes = set()
        for filename in self.found_files:
            # Find the part after the run identifier
            for expected in EXPECTED_FILES:
                if filename.endswith(expected):
                    found_suffixes.add(expected)
                    break

        return [f for f in EXPECTED_FILES if f not in found_suffixes]

    @property
    def is_complete(self) -> bool:
        """Check if this run has all required files."""
        return len(self.missing_files) == 0

    @property
    def num_found(self) -> int:
        """Number of expected files found."""
        return len(EXPECTED_FILES) - len(self.missing_files)

    @property
    def run_key(self) -> str:
        """Unique key for this run (task + run_id)."""
        return f'task-{self.task}_{self.run_id}'


@dataclass
class SessionResult:
    """Results for a single subject/session."""
    subject: str
    session: str
    folder_exists: bool
    runs: dict[str, RunResult]  # run_key -> RunResult

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
        """Check if session is complete (has runs and all are complete)."""
        return self.folder_exists and self.num_runs > 0 and all(r.is_complete for r in self.runs.values())

    @property
    def is_missing(self) -> bool:
        """Check if session is missing (no folder or no runs or all runs incomplete)."""
        if not self.folder_exists:
            return True
        if self.num_runs == 0:
            return True
        # All runs incomplete
        return self.complete_runs == 0

    def get_runs_by_task(self, task_pattern: str) -> dict[str, RunResult]:
        """Get runs matching a task pattern."""
        return {
            key: r for key, r in self.runs.items()
            if task_pattern.lower() in r.task.lower()
        }

    def has_task(self, task_pattern: str) -> bool:
        """Check if session has any runs for this task."""
        return len(self.get_runs_by_task(task_pattern)) > 0

    def get_expected_run_count(self, task_pattern: str) -> int:
        """Get expected number of runs for a task."""
        if 'floc' in task_pattern.lower():
            return 10
        elif 'ret' in task_pattern.lower():
            # Check if retFF or retfixFF exists
            task_runs = self.get_runs_by_task('ret')
            has_retff = any(
                'retff' in r.task.lower() or 'retfixff' in r.task.lower()
                for r in task_runs.values()
            )
            return 6 if has_retff else 4
        return 0

    def has_enough_runs(self, task_pattern: str) -> bool:
        """Check if task has enough runs (considering both count and completeness)."""
        task_runs = self.get_runs_by_task(task_pattern)
        if not task_runs:
            return False

        expected_count = self.get_expected_run_count(task_pattern)
        actual_count = len(task_runs)

        return actual_count >= expected_count

    def is_task_complete(self, task_pattern: str) -> bool:
        """Check if all runs for this task are complete AND has enough runs."""
        task_runs = self.get_runs_by_task(task_pattern)
        if not task_runs:
            return False

        # Check if has enough runs
        if not self.has_enough_runs(task_pattern):
            return False

        # Check if all runs are complete
        return all(r.is_complete for r in task_runs.values())

    def is_task_missing(self, task_pattern: str) -> bool:
        """Check if this task has no complete runs OR not enough runs."""
        task_runs = self.get_runs_by_task(task_pattern)
        if not task_runs:
            return True

        # Not enough runs counts as missing
        if not self.has_enough_runs(task_pattern):
            return True

        # All runs incomplete
        return all(not r.is_complete for r in task_runs.values())

    @property
    def session_id(self) -> str:
        """Get session identifier."""
        return f'{self.subject}/{self.session}'

    def get_task_names(self) -> set[str]:
        """Get all unique task names in this session."""
        return {r.task for r in self.runs.values()}

    def get_task_summary(self, task_pattern: str) -> str:
        """Get a summary string for a task."""
        task_runs = self.get_runs_by_task(task_pattern)
        if not task_runs:
            return 'no runs'

        complete = sum(1 for r in task_runs.values() if r.is_complete)
        expected = self.get_expected_run_count(task_pattern)
        actual = len(task_runs)

        return f'{complete}/{actual} complete (expected: {expected})'


def extract_run_info(filename: str) -> tuple[str, str, str]:
    """
    Extract run prefix, task, and run ID from a filename.

    Example: sub-02_ses-02_task-retRW_run-02_desc-brain_mask.nii.gz
             -> ("run-02", "sub-02_ses-02_task-retRW_run-02", "retRW")

    Returns:
        Tuple of (run_id, run_prefix, task)
    """
    parts = filename.split('_')

    # Find the run and task parts
    run_id = None
    task = None
    prefix_parts = []

    for part in parts:
        prefix_parts.append(part)
        if part.startswith('task-'):
            task = part.replace('task-', '')
        if part.startswith('run-'):
            run_id = part
            break

    if run_id:
        run_prefix = '_'.join(prefix_parts)
        return run_id, run_prefix, task or 'unknown'

    return '', '', ''


def get_expected_combinations() -> list[tuple[str, str]]:
    """Generate all expected sub/ses combinations."""
    combinations = []
    for sub_id in range(1, 12):  # 01-11
        for ses_id in range(1, 11):  # 01-10
            sub_str = f'sub-{sub_id:02d}'
            ses_str = f'ses-{ses_id:02d}'
            combinations.append((sub_str, ses_str))
    return combinations


def check_fmriprep_outputs(
    func_dir: Path,
    subject: str,
    session: str,
) -> dict[str, RunResult]:
    """
    Check fMRIPrep outputs for a given subject/session.

    Returns:
        Dictionary mapping run_key (task-XX_run-XX) to RunResult
    """
    if not func_dir.exists():
        return {}

    # Group files by task+run combination (not just run!)
    runs_data = defaultdict(lambda: {'files': set(), 'task': None, 'run_id': None})

    for file_path in func_dir.iterdir():
        if not file_path.is_file():
            continue

        filename = file_path.name

        # Extract run information
        run_id, run_prefix, task = extract_run_info(filename)

        if run_id and task:
            # Use composite key: task + run_id
            run_key = f'task-{task}_{run_id}'
            runs_data[run_key]['files'].add(filename)
            runs_data[run_key]['task'] = task
            runs_data[run_key]['run_id'] = run_id

    # Convert to RunResult objects
    results = {}
    for run_key, data in runs_data.items():
        results[run_key] = RunResult(
            run_id=data['run_id'],
            task=data['task'],
            found_files=data['files'],
        )

    return results


def check_single_session(fmriprep_dir: Path, subject: str, session: str) -> SessionResult:
    """Check a single session."""
    func_dir = fmriprep_dir / subject / session / 'func'
    folder_exists = func_dir.exists()

    if folder_exists:
        runs = check_fmriprep_outputs(func_dir, subject, session)
    else:
        runs = {}

    return SessionResult(
        subject=subject,
        session=session,
        folder_exists=folder_exists,
        runs=runs,
    )


def check_all_sessions(fmriprep_dir: Path) -> list[SessionResult]:
    """Check fMRIPrep outputs across all subjects/sessions."""
    expected = get_expected_combinations()
    results = []

    with typer.progressbar(
        expected,
        label='Checking fMRIPrep outputs',
        show_pos=True,
        length=len(expected),
    ) as progress:
        for sub, ses in progress:
            results.append(check_single_session(fmriprep_dir, sub, ses))

    return results


def debug_session(fmriprep_dir: Path, session_id: str):
    """Debug a specific session."""
    parts = session_id.split('/')
    if len(parts) != 2:
        console.print('[red]Invalid session format. Use: sub-XX/ses-XX[/red]')
        return

    sub, ses = parts
    console.print(f'\n[bold]Debug info for {session_id}:[/bold]\n')

    session_result = check_single_session(fmriprep_dir, sub, ses)

    console.print('[cyan]Basic Info:[/cyan]')
    console.print(f'  Folder exists: {session_result.folder_exists}')
    console.print(f"  Folder path: {fmriprep_dir / sub / ses / 'func'}")
    console.print(f'  Total runs found: {session_result.num_runs}')
    console.print(f'  Complete runs: {session_result.complete_runs}')

    if not session_result.folder_exists:
        console.print('\n[red]Func folder does not exist![/red]')
        return

    if session_result.num_runs == 0:
        console.print('\n[red]No runs found in folder![/red]')
        return

    console.print('\n[cyan]Runs Detail:[/cyan]')
    for run_key, run in sorted(session_result.runs.items()):
        status = '[green]COMPLETE[/green]' if run.is_complete else '[red]INCOMPLETE[/red]'
        console.print(f'  {run_key}: {run.num_found}/{len(EXPECTED_FILES)} files - {status}')

        if not run.is_complete:
            console.print(f'    [yellow]Missing files ({len(run.missing_files)}):[/yellow]')
            for missing_file in run.missing_files:
                console.print(f'      - {missing_file}')

    console.print('\n[cyan]Task Analysis:[/cyan]')
    all_tasks = session_result.get_task_names()
    console.print(f"  All tasks found: {', '.join(sorted(all_tasks))}")

    console.print('\n  [bold]Retinotopy (ret) analysis:[/bold]')
    console.print(f"    Has ret tasks: {session_result.has_task('ret')}")
    if session_result.has_task('ret'):
        ret_runs = session_result.get_runs_by_task('ret')
        expected_ret = session_result.get_expected_run_count('ret')
        console.print(f'    Number of ret runs: {len(ret_runs)} (expected: {expected_ret})')
        console.print(f"    Has enough runs: {session_result.has_enough_runs('ret')}")
        console.print(f"    Ret complete: {session_result.is_task_complete('ret')}")
        console.print(f"    Ret missing: {session_result.is_task_missing('ret')}")

        # Show status of each ret run
        for run_key, run in sorted(ret_runs.items()):
            status = '✓' if run.is_complete else '✗'
            console.print(f'      {status} {run_key}: {run.num_found}/{len(EXPECTED_FILES)}')

    console.print('\n  [bold]Functional Localizer (floc) analysis:[/bold]')
    console.print(f"    Has floc tasks: {session_result.has_task('floc')}")
    if session_result.has_task('floc'):
        floc_runs = session_result.get_runs_by_task('floc')
        expected_floc = session_result.get_expected_run_count('floc')
        console.print(f'    Number of floc runs: {len(floc_runs)} (expected: {expected_floc})')
        console.print(f"    Has enough runs: {session_result.has_enough_runs('floc')}")
        console.print(f"    Floc complete: {session_result.is_task_complete('floc')}")
        console.print(f"    Floc missing: {session_result.is_task_missing('floc')}")

        # Show status of each floc run
        for run_key, run in sorted(floc_runs.items()):
            status = '✓' if run.is_complete else '✗'
            console.print(f'      {status} {run_key}: {run.num_found}/{len(EXPECTED_FILES)}')

    console.print('\n[cyan]Session Classification:[/cyan]')
    console.print(f'  Overall complete: {session_result.is_complete}')
    console.print(f'  Overall missing: {session_result.is_missing}')


def print_task_summary(results: list[SessionResult], task_pattern: str, task_name: str):
    """Print summary for a specific task compared to all 110 sessions."""
    total_sessions = len(results)  # Should be 110

    # Categorize all 110 sessions for this task
    complete_sessions = []
    no_data_sessions = []  # No folder or no runs for this task
    incomplete_sessions = []  # Has task runs but all incomplete OR not enough runs
    partial_sessions = []  # Has task runs, some complete, some incomplete

    for r in results:
        if not r.folder_exists or not r.has_task(task_pattern):
            no_data_sessions.append(r)
        elif r.is_task_complete(task_pattern):
            complete_sessions.append(r)
        elif r.is_task_missing(task_pattern):
            incomplete_sessions.append(r)
        else:
            partial_sessions.append(r)

    # Calculate total missing (no data + incomplete runs)
    total_missing = len(no_data_sessions) + len(incomplete_sessions)

    # Count runs
    sessions_with_task = [r for r in results if r.has_task(task_pattern)]
    total_runs = sum(len(r.get_runs_by_task(task_pattern)) for r in sessions_with_task)
    complete_runs = sum(
        sum(1 for run in r.get_runs_by_task(task_pattern).values() if run.is_complete)
        for r in sessions_with_task
    )

    console.print(f'  Total sessions: {total_sessions}')
    console.print(f'  [green]✓ Complete sessions: {len(complete_sessions)}[/green]')
    console.print(f'  [yellow]◐ Partial sessions: {len(partial_sessions)}[/yellow]')
    console.print(f'  [red]✗ Missing sessions: {total_missing}[/red]')
    console.print(f'    - No data: {len(no_data_sessions)}')
    console.print(f'    - Incomplete/Not enough runs: {len(incomplete_sessions)}')
    console.print(
        f'  Total runs found: {total_runs} ([green]{complete_runs} complete[/green], '
        f'[red]{total_runs - complete_runs} incomplete[/red])',
    )

    # Show missing sessions details
    if total_missing > 0:
        console.print(f'\n  [bold red]Missing {task_name} sessions ({total_missing}):[/bold red]')

        if no_data_sessions:
            console.print(f'\n    [yellow]No data ({len(no_data_sessions)}):[/yellow]')
            for r in sorted(no_data_sessions, key=lambda x: x.session_id):
                if not r.folder_exists:
                    console.print(f'      {r.session_id} (no func folder)')
                else:
                    # Show what tasks ARE present
                    task_names = r.get_task_names()
                    if task_names:
                        console.print(
                            f"      {r.session_id} (no {task_name} runs, has: {', '.join(sorted(task_names))})",
                        )
                    else:
                        console.print(f'      {r.session_id} (no runs at all)')

        if incomplete_sessions:
            console.print(
                f'\n    [yellow]Incomplete/Not enough runs ({len(incomplete_sessions)}):[/yellow]',
            )
            for r in sorted(incomplete_sessions, key=lambda x: x.session_id):
                task_runs = r.get_runs_by_task(task_pattern)
                expected = r.get_expected_run_count(task_pattern)
                actual = len(task_runs)
                complete = sum(1 for run in task_runs.values() if run.is_complete)

                reason = []
                if actual < expected:
                    reason.append(f'not enough runs: {actual}/{expected}')
                if complete == 0 and actual > 0:
                    reason.append('all incomplete')

                console.print(f"      {r.session_id} ({', '.join(reason)})")


def print_summary(results: list[SessionResult]):
    """Print task-specific summaries."""
    console.print('\n[bold]Task-Specific Summaries:[/bold]')

    console.print('\n[cyan]Retinotopy (ret) Tasks:[/cyan]')
    print_task_summary(results, 'ret', 'ret')

    console.print('\n[cyan]Functional Localizer (floc) Tasks:[/cyan]')
    print_task_summary(results, 'floc', 'floc')


def print_cross_task_analysis(results: list[SessionResult]):
    """Print analysis of sessions missing across tasks."""
    console.print('\n[bold]Cross-Task Analysis:[/bold]')

    # Categorize all 110 sessions
    sessions_by_status = {
        'complete_both': [],
        'no_data': [],
        'missing_both': [],
        'missing_only_ret': [],
        'missing_only_floc': [],
        'partial': [],
    }

    for r in results:
        has_no_data = not r.folder_exists or r.num_runs == 0

        if has_no_data:
            sessions_by_status['no_data'].append(r)
        else:
            has_ret = r.has_task('ret')
            has_floc = r.has_task('floc')
            ret_complete = r.is_task_complete('ret') if has_ret else False
            floc_complete = r.is_task_complete('floc') if has_floc else False
            ret_missing = not has_ret or r.is_task_missing('ret')
            floc_missing = not has_floc or r.is_task_missing('floc')

            if ret_complete and floc_complete:
                sessions_by_status['complete_both'].append(r)
            elif ret_missing and floc_missing:
                sessions_by_status['missing_both'].append(r)
            elif ret_missing:
                sessions_by_status['missing_only_ret'].append(r)
            elif floc_missing:
                sessions_by_status['missing_only_floc'].append(r)
            else:
                sessions_by_status['partial'].append(r)

    console.print(
        f"\n  [bold green]Complete for both tasks:[/bold green] {len(sessions_by_status['complete_both'])}",
    )

    console.print(
        f"\n  [bold red]Sessions with no data:[/bold red] {len(sessions_by_status['no_data'])}",
    )
    if sessions_by_status['no_data']:
        for r in sorted(sessions_by_status['no_data'], key=lambda x: x.session_id):
            console.print(f'    {r.session_id}')

    console.print(
        f"\n  [bold red]Sessions missing BOTH tasks:[/bold red] {len(sessions_by_status['missing_both'])}",
    )
    if sessions_by_status['missing_both']:
        for r in sorted(sessions_by_status['missing_both'], key=lambda x: x.session_id):
            console.print(f'    {r.session_id}')

    console.print(
        f"\n  [bold yellow]Sessions missing ONLY ret:[/bold yellow] {len(sessions_by_status['missing_only_ret'])}",
    )
    if sessions_by_status['missing_only_ret']:
        for r in sorted(sessions_by_status['missing_only_ret'], key=lambda x: x.session_id):
            ret_summary = r.get_task_summary('ret')
            console.print(f'    {r.session_id} (ret: {ret_summary})')

    console.print(
        f"\n  [bold yellow]Sessions missing ONLY floc:[/bold yellow] {len(sessions_by_status['missing_only_floc'])}",
    )
    if sessions_by_status['missing_only_floc']:
        for r in sorted(sessions_by_status['missing_only_floc'], key=lambda x: x.session_id):
            floc_summary = r.get_task_summary('floc')
            console.print(f'    {r.session_id} (floc: {floc_summary})')

    console.print(
        f"\n  [bold cyan]Sessions with partial completion:[/bold cyan] {len(sessions_by_status['partial'])}",
    )
    if sessions_by_status['partial']:
        for r in sorted(sessions_by_status['partial'], key=lambda x: x.session_id):
            ret_summary = r.get_task_summary('ret')
            floc_summary = r.get_task_summary('floc')
            console.print(f'    {r.session_id} (ret: {ret_summary}, floc: {floc_summary})')


def print_run_distribution(results: list[SessionResult]):
    """Print distribution of runs per session."""
    # Overall distribution
    run_counts = defaultdict(int)
    for result in results:
        if result.num_runs > 0:
            run_counts[result.num_runs] += 1

    if run_counts:
        console.print('\n[bold]Overall Run Distribution:[/bold]')
        table = Table(show_header=True, header_style='bold magenta')
        table.add_column('Number of Runs', style='cyan')
        table.add_column('Number of Sessions', style='green')

        for num_runs in sorted(run_counts.keys()):
            table.add_row(str(num_runs), str(run_counts[num_runs]))

        console.print(table)


@app.command()
def main(
    fmriprep_dir: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=False,
        dir_okay=True,
        help='Path to fMRIPrep output directory containing sub-XX/ses-XX/func folders',
    ),
    show_distribution: bool = typer.Option(
        True,
        '--show-distribution/--no-distribution',
        help='Show distribution of runs per session',
    ),
    debug: str = typer.Option(
        None,
        '--debug',
        '-d',
        help="Debug a specific session (e.g., 'sub-02/ses-08') - skips full scan",
    ),
):
    """
    Check fMRIPrep preprocessing outputs for completeness.

    For each subject/session, this script:
    1. Discovers all unique runs in the func directory
    2. Verifies each run has all required output files
    3. Checks if task has minimum required number of runs:
       - floc: at least 10 runs
       - ret: at least 4 runs (6 if retFF/retfixFF present)
    4. Reports completeness by task (ret, floc) out of 110 total sessions
    5. Shows cross-task analysis of missing sessions

    Session categories per task:
    - Complete: All task runs have all files AND has enough runs
    - Partial: Some task runs complete, some incomplete
    - Missing: No data, all task runs incomplete, OR not enough runs

    Example:
        check_fmriprep.py /path/to/fmriprep
        check_fmriprep.py /path/to/fmriprep --debug sub-02/ses-08
    """
    # Debug mode - skip full scan
    if debug:
        debug_session(fmriprep_dir, debug)
        return

    # Normal mode - full scan
    console.print('[bold]Checking fMRIPrep outputs[/bold]')
    console.print(f'  fMRIPrep dir: {fmriprep_dir}')
    console.print(f'  Expected files per run: {len(EXPECTED_FILES)}')
    console.print('  Minimum runs - floc: 10, ret: 4-6 (depends on presence of retFF)\n')

    results = check_all_sessions(fmriprep_dir)

    print_summary(results)

    print_cross_task_analysis(results)

    if show_distribution:
        print_run_distribution(results)


if __name__ == '__main__':
    app()
