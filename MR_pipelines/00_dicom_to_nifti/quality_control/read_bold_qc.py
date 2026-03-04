#!/usr/bin/env python3
"""
Check for unmatched BOLD and SBRef files from BIDS dataframe.

Usage:
    python check_sbref_matches.py --csv bids_files.csv
"""

import typer
import pandas as pd
from pathlib import Path
from collections import defaultdict
from rich.console import Console
from rich.table import Table

app = typer.Typer()
console = Console()


@app.command()
def check(
    csv_file: Path = typer.Option(..., "--csv", "-c", help="CSV file with BIDS info"),
    show_ok: bool = typer.Option(False, "--show-ok", help="Show sessions with all matches"),
):
    """Check for unmatched BOLD and SBRef files."""
    
    console.print(f"[bold]Reading CSV:[/bold] {csv_file}\n")
    
    # Read CSV
    df = pd.read_csv(csv_file)
    
    # Filter to functional data only
    func_df = df[df['modality'].isin(['bold'])].copy()
    
    console.print(f"Found {len(func_df)} functional files")
    console.print(f"  BOLD: {len(func_df[func_df['modality'] == 'bold'])}")
    
    # Group by session
    sessions = func_df.groupby(['sub', 'ses'])
    
    issues = []
    ok_sessions = []
    
    for (sub, ses), group in sessions:
        bold_files = group[group['modality'] == 'bold']     
        # Check for unmatched BOLDs (where sbref column is False/None/NaN)
        unmatched_bolds = []
        for _, row in bold_files.iterrows():
            if not row['task']=='WC':
                sbref_status = row.get('sbref')

                if pd.isna(sbref_status) or sbref_status == False or sbref_status is None:
                    unmatched_bolds.append({
                        'task': row['task'],
                        'run': row['run'],
                        'filepath': row['filepath'],
                    })
        
        # Count total
        n_bold = len(bold_files)
        n_unmatched = len(unmatched_bolds)
        
        if unmatched_bolds:
            issues.append({
                'sub': sub,
                'ses': ses,
                'n_bold': n_bold,
                'n_unmatched': n_unmatched,
                'unmatched_bolds': unmatched_bolds,
            })
        else:
            ok_sessions.append({
                'sub': sub,
                'ses': ses,
                'n_bold': n_bold,
            })
    
    # Print summary
    console.print(f"[bold]Summary:[/bold]")
    console.print(f"  Total sessions: {len(sessions)}")
    console.print(f"  [green]OK: {len(ok_sessions)}[/green]")
    console.print(f"  [red]Issues: {len(issues)}[/red]\n")
    
    # Print sessions with issues
    if issues:
        console.print(f"[red bold]Sessions with Unmatched BOLD/SBRef:[/red bold]\n")
        
        for item in issues:
            console.print(f"[cyan]sub-{item['sub']:02d} / ses-{item['ses']:02d}[/cyan]")
            console.print(f"  BOLD: {item['n_bold']}, Unmatched: {item['n_unmatched']}")
            
            if item['unmatched_bolds']:
                console.print(f"  [red]Unmatched BOLD files:[/red]")
                for bold in item['unmatched_bolds']:
                    console.print(f"    - task-{bold['task']} run-{bold['run']:02d}")
                    console.print(f"      {Path(bold['filepath']).name}")
            
            console.print()
    
    # Print OK sessions if requested
    if show_ok and ok_sessions:
        console.print(f"[green bold]Sessions with All Matches ({len(ok_sessions)}):[/green bold]\n")
        
        table = Table()
        table.add_column("Subject", style="cyan")
        table.add_column("Session", style="cyan")
        table.add_column("BOLD", justify="right")
        table.add_column("SBRef", justify="right")
        
        for item in ok_sessions:
            table.add_row(
                f"sub-{item['sub']:02d}",
                f"ses-{item['ses']:02d}",
                str(item['n_bold']),
                str(item['n_sbref']),
            )
        
        console.print(table)
    
    # Write detailed report
    if issues:
        output_file = Path("sbref_mismatch_report.txt")
        with open(output_file, 'w') as f:
            f.write("SBRef Mismatch Report\n")
            f.write("=" * 80 + "\n\n")
            
            for item in issues:
                f.write(f"sub-{item['sub']:02d} / ses-{item['ses']:02d}\n")
                f.write(f"  BOLD files: {item['n_bold']}\n")
                f.write(f"  SBRef files: {item['n_sbref']}\n")
                f.write(f"  Unmatched BOLD: {item['n_unmatched']}\n\n")
                
                if item['unmatched_bolds']:
                    f.write("  Unmatched BOLD files:\n")
                    for bold in item['unmatched_bolds']:
                        f.write(f"    - task-{bold['task']} run-{bold['run']:02d}\n")
                        f.write(f"      {bold['filepath']}\n")
                
                f.write("\n")
        
        console.print(f"[green]✓ Detailed report saved:[/green] {output_file}")


if __name__ == "__main__":
    app()