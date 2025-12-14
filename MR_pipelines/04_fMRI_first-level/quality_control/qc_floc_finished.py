"""
Check the number of files in each subject/session l1_surface directory
and report any that don't have exactly 322 files.
"""

import pandas as pd
from pathlib import Path
import typer

app = typer.Typer()


@app.command()
def check_l1_surface_files(
    l1_surface_dir: Path = typer.Argument(..., help="Path to l1_surface directory"),
    expected_count: int = typer.Option(321, help="Expected number of files per directory"),
        output_csv: Path = typer.Option("missing_sub.txt", help="Output CSV file path")
):
    """
    Loop through l1_surface directory and count files in each sub/ses folder.
    Generate a report marking directories with incorrect file counts.
    """
    
    results = []
    
    # Find all subdirectories (assuming structure: l1_surface/sub-XX/ses-YY/)
    for sub_dir in sorted(l1_surface_dir.glob("sub-*")):
        if sub_dir.is_dir():
            for ses_dir in sorted(sub_dir.glob("ses-*")):
                if ses_dir.is_dir():
                    # Count all files (not directories) in this session
                    num_files = len([f for f in ses_dir.iterdir() if f.is_file()])
                    
                    results.append({
                        'sub': sub_dir.name.replace('sub-', ''),
                        'ses': ses_dir.name.replace('ses-', ''),
                        'num_files': num_files,
                        'status': 'OK' if num_files == expected_count else 'WRONG',
                        'wrong': num_files != expected_count
                    })
    
    # Create DataFrame
    df = pd.DataFrame(results)
    
    # Save to CSV
    df.to_csv(output_csv, index=False)
    
    # Print summary
    typer.echo(f"\n{'='*60}")
    typer.echo(f"L1 Surface Files Report")
    typer.echo(f"{'='*60}")
    typer.echo(f"Total directories checked: {len(df)}")
    typer.echo(f"Directories with correct count ({expected_count} files): {(df['num_files'] == expected_count).sum()}")
    typer.secho(f"Directories with WRONG count: {(df['num_files'] != expected_count).sum()}", 
                fg=typer.colors.RED if (df['num_files'] != expected_count).sum() > 0 else typer.colors.GREEN)
    typer.echo(f"\nReport saved to: {output_csv}")
    
    # Show directories with wrong counts
    wrong_dirs = df[df['wrong'] == True]
    if len(wrong_dirs) > 0:
        typer.echo(f"\n{'='*60}")
        typer.secho("Directories with WRONG file counts:", fg=typer.colors.RED)
        typer.echo(f"{'='*60}")
        typer.echo(wrong_dirs.to_string(index=False))
    else:
        typer.secho("\n✓ All directories have the correct number of files!", fg=typer.colors.GREEN)
    
    return df


if __name__ == "__main__":
    app()