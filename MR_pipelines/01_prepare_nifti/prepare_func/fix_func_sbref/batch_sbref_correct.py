#!/usr/bin/env python3
"""
Batch call drop_extra_sbref.py for multiple subjects/sessions.

Usage:
    python batch_drop_sbref.py --bids /path/to/bids --subseslist subseslist.txt --func-type bold --dry-run
    python batch_drop_sbref.py --bids /path/to/bids --subseslist subseslist.txt --func-type bold --execute
"""

import typer
import subprocess
from pathlib import Path
from rich.console import Console
from rich.progress import track

app = typer.Typer()
console = Console()


def read_subseslist(filepath: Path) -> list[tuple[str, str]]:
    """Read subject/session pairs from file."""
    combos = []
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("sub"):
                continue
            parts = line.split(",")
            if len(parts) >= 2:
                sub = parts[0].replace("sub-", "")
                ses = parts[1].replace("ses-", "")
                combos.append((sub, ses))
    return combos


@app.command()
def batch(
    bids_dir: Path = typer.Option(..., "--bids", "-b"),
    subseslist: Path = typer.Option(..., "--subseslist"),
    func_type: str = typer.Option("bold", "--func-type", "-f"),
    max_gap: int = typer.Option(60, "--max-gap"),
    drop_script: Path = typer.Option(
        "/bcbl/home/home_n-z/tlei/soft/launchcontainers/MR_pipelines/01_prepare_nifti/prepare_func/fix_func_sbref/drop_extra_sbref.py",
        "--script",
        help="Path to drop_extra_sbref.py",
    ),
    dry_run: bool = typer.Option(True, "--dry-run/--execute"),
):
    """Call drop_extra_sbref.py for each subject/session in subseslist."""

    console.print("[bold]Batch processing SBRef files[/bold]")
    console.print(f"BIDS dir: {bids_dir}")
    console.print(f"Func type: {func_type}")
    console.print(f"Dry run: {dry_run}\n")

    # Read subseslist
    combos = read_subseslist(subseslist)
    console.print(f"Processing {len(combos)} sessions\n")

    # Call drop_extra_sbref.py for each session
    for sub, ses in track(combos, description="Processing..."):
        cmd = [
            "python",
            str(drop_script),
            "--bids",
            str(bids_dir),
            "--sub",
            sub,
            "--ses",
            ses,
            "--func-type",
            func_type,
            "--max-gap",
            str(max_gap),
        ]

        if not dry_run:
            cmd.append("--execute")
        else:
            cmd.append("--dry-run")

        # Run command
        try:
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode != 0:
                console.print(f"[red]✗ Error for sub-{sub}/ses-{ses}[/red]")
                console.print(result.stderr)
            else:
                # Only show output if there were renames
                if "Rename plan" in result.stdout:
                    console.print(f"[yellow]sub-{sub}/ses-{ses}:[/yellow]")
                    console.print(result.stdout)

        except Exception as e:
            console.print(f"[red]✗ Failed for sub-{sub}/ses-{ses}: {e}[/red]")

    console.print("\n[green]✓ Batch processing complete[/green]")


if __name__ == "__main__":
    app()
