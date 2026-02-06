#!/usr/bin/env python3
"""
Check for missing subject/session combinations in PRF analysis folder.
"""
from pathlib import Path
from typing import List, Tuple

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer()
console = Console()

def get_expected_combinations() -> List[Tuple[str, str]]:
    """Generate all expected sub/ses combinations."""
    combinations = []
    for sub_id in range(1, 12):  # 01-11
        for ses_id in range(1, 11):  # 01-10
            sub_str = f"sub-{sub_id:02d}"
            ses_str = f"ses-{ses_id:02d}"
            combinations.append((sub_str, ses_str))
    return combinations


def check_analysis_folder(analysis_dir: Path) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    """
    Check which subject/session combinations exist and which are missing.
    
    Returns:
        Tuple of (existing, missing) combinations
    """
    expected = get_expected_combinations()
    existing = []
    missing = []
    
    with typer.progressbar(
        expected, 
        label="Checking sub/ses combinations",
        show_pos=True,
        length=len(expected)
    ) as progress:
        for sub, ses in progress:
            sub_ses_path = analysis_dir / sub / ses
            if sub_ses_path.exists():
                existing.append((sub, ses))
            else:
                missing.append((sub, ses))
    
    return existing, missing


def print_results(existing: List[Tuple[str, str]], missing: List[Tuple[str, str]]):
    """Print summary of existing and missing combinations."""
    console.print(f"\n[green]Found: {len(existing)}/110 combinations[/green]")
    console.print(f"[red]Missing: {len(missing)}/110 combinations[/red]\n")
    
    if missing:
        console.print("[bold red]Missing Subject/Session Combinations:[/bold red]")
        
        # Group by subject for cleaner output
        missing_by_sub = {}
        for sub, ses in missing:
            if sub not in missing_by_sub:
                missing_by_sub[sub] = []
            missing_by_sub[sub].append(ses)
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Subject", style="cyan")
        table.add_column("Missing Sessions", style="yellow")
        
        for sub in sorted(missing_by_sub.keys()):
            sessions = ", ".join(missing_by_sub[sub])
            table.add_column
            table.add_row(sub, sessions)
        
        console.print(table)
        
        # Also print simple list for easy copying
        console.print("\n[bold]Simple list:[/bold]")
        for sub, ses in missing:
            console.print(f"  {sub}/{ses}")
    else:
        console.print("[bold green]✓ All 110 combinations present![/bold green]")


@app.command()
def main(
    analysis_dir: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=False,
        dir_okay=True,
        help="Path to analysis directory containing sub-XX/ses-XX folders"
    )
):
    """
    Check for missing subject/session combinations in PRF analysis folder.
    
    Expects structure: analysis_dir/sub-XX/ses-XX/
    where XX ranges from 01-11 for subjects and 01-10 for sessions.
    """
    console.print(f"[bold]Checking analysis directory:[/bold] {analysis_dir}")
    
    existing, missing = check_analysis_folder(analysis_dir)
    print_results(existing, missing)


if __name__ == "__main__":
    app()