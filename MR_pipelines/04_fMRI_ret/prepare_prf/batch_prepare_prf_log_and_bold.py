#!/usr/bin/env python3
import subprocess
from pathlib import Path
from datetime import datetime
import typer
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def batch(
    bids_dir: Path = typer.Option(..., "--bids", "-b"),
    subseslist: Path = typer.Option(..., "--list", "-l"),
    command: str = typer.Option(..., "--command", "-c", help="link, check, or rename"),
    force: bool = typer.Option(False, "--force", "-f"),
    dry_run: bool = typer.Option(True, "--dry-run/--execute"),
    log_dir: Path = typer.Option("logs", "--log-dir"),
):
    """Batch process subjects/sessions from subseslist.txt"""
    
    # Create log directory
    log_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"{command}_{timestamp}.log"
    
    script_path = '/scratch/tlei/lc/MR_pipelines/04_fMRI_ret/prepare_prf/prepare_prf_log_and_bold.py'
    console.print(f"[cyan]Batch processing: {command}[/cyan]")
    console.print(f"[cyan]Log file: {log_file}[/cyan]\n")
    
    # Read subseslist
    with open(subseslist) as f:
        pairs = [line.strip().split(',') for line in f]
    
    results = []
    
    with open(log_file, 'w') as log:
        log.write(f"Batch {command} - {timestamp}\n")
        log.write(f"BIDS directory: {bids_dir}\n")
        log.write(f"Dry run: {dry_run}\n")
        log.write("="*80 + "\n\n")
        
        for sub, ses in pairs:
            console.print(f"[yellow]Processing sub-{sub} ses-{ses}[/yellow]")
            log.write(f"\n{'='*80}\n")
            log.write(f"Processing sub-{sub} ses-{ses}\n")
            log.write(f"{'='*80}\n")
            
            # Build command
            cmd = [
                "python", script_path, command,
                "--bids", str(bids_dir),
                "--sub", sub,
                "--ses", ses
            ]
            
            if command == "link":
                if force:
                    cmd.append("--force")
                if not dry_run:
                    cmd.append("--execute")
            elif command == "rename":
                if not dry_run:
                    cmd.append("--execute")
            
            # Run command
            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                log.write(result.stdout)
                if result.stderr:
                    log.write(f"\nSTDERR:\n{result.stderr}\n")
                
                console.print(f"[green]✓ Completed sub-{sub} ses-{ses}[/green]\n")
                results.append((sub, ses, "SUCCESS"))
                
            except subprocess.CalledProcessError as e:
                log.write(f"\nERROR:\n{e.stderr}\n")
                console.print(f"[red]✗ Failed sub-{sub} ses-{ses}[/red]\n")
                results.append((sub, ses, "FAILED"))
        
        # Summary
        log.write(f"\n\n{'='*80}\n")
        log.write("SUMMARY\n")
        log.write(f"{'='*80}\n")
        
        success = sum(1 for _, _, status in results if status == "SUCCESS")
        failed = sum(1 for _, _, status in results if status == "FAILED")
        
        log.write(f"Total: {len(results)}\n")
        log.write(f"Success: {success}\n")
        log.write(f"Failed: {failed}\n")
        
        if failed > 0:
            log.write(f"\nFailed sessions:\n")
            for sub, ses, status in results:
                if status == "FAILED":
                    log.write(f"  sub-{sub} ses-{ses}\n")
    
    console.print(f"\n[cyan]Summary:[/cyan]")
    console.print(f"  Total: {len(results)}")
    console.print(f"  Success: {success}")
    console.print(f"  Failed: {failed}")
    console.print(f"\n[cyan]Log saved to: {log_file}[/cyan]")


if __name__ == "__main__":
    app()

