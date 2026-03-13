#!/usr/bin/env python3
import typer
from pathlib import Path
from rich.console import Console

app = typer.Typer()
console = Console()


def should_drop(task: str, competing_tasks: list):
    """
    Determine if a task should be dropped based on priority.
    Priority: retRW > retfixRW, retFF > retfixFF, retCB > retfixRWblock*
    Keep the simpler task names, drop the retfix* versions.
    """
    priority_map = {
        'retRW': ['retfixRW'],
        'retFF': ['retfixFF'],
        'retCB': [],  # Will handle retfixRWblock* separately
    }
    
    # Check exact matches - drop retfix versions
    for keep_task, drop_tasks in priority_map.items():
        if task in drop_tasks and keep_task in competing_tasks:
            return True  # Drop this retfix task
    
    # Drop any retfixRWblock* if retCB exists
    if task.startswith('retfixRWblock') and 'retCB' in competing_tasks:
        return True
    
    return False





@app.command()
def drop_duplicates(
    duplicates_file: Path = typer.Option("duplicates.txt", "--input", "-i"),
    dry_run: bool = typer.Option(True, "--dry-run/--execute"),
):
    """Drop duplicate files based on task priority rules."""
    
    console.print(f"[cyan]Reading duplicates from {duplicates_file}[/cyan]")
    console.print(f"[yellow]Dry run: {dry_run}[/yellow]\n")
    
    # Read duplicates file
    duplicates = []
    with open(duplicates_file) as f:
        lines = f.readlines()[1:]  # Skip header
        for line in lines:
            parts = line.strip().split('\t')
            if len(parts) == 6:
                duplicates.append({
                    'session': parts[0],
                    'time': parts[1],
                    'task': parts[2],
                    'run': parts[3],
                    'type': parts[4],
                    'file': parts[5]
                })
    
    # Group by session + time
    from collections import defaultdict
    groups = defaultdict(list)
    for dup in duplicates:
        key = f"{dup['session']}_{dup['time']}"
        groups[key].append(dup)
    
    # Determine what to drop
    to_drop = []
    
    for key, group in groups.items():
        # Get all tasks in this group
        tasks_in_group = [item['task'] for item in group]
        for item in group:
            if should_drop(item['task'], tasks_in_group):
                to_drop.append(item)
    
    if not to_drop:
        console.print("[green]No files to drop based on priority rules[/green]")
        return
    
    console.print(f"[red]Files to drop ({len(to_drop)}):[/red]\n")
    
    for item in to_drop:
        console.print(f"[red]DROP:[/red] {item['task']} - {Path(item['file']).name}")
        # In drop_duplicates command, update deletion part:

        if not dry_run:
            # Delete both .nii.gz and .json
            file_path = Path(item['file'])
            
            # Delete the main file
            if file_path.exists():
                file_path.unlink()
                console.print(f"  [red]Deleted: {file_path.name}[/red]")
            
            # Delete the JSON
            json_file = file_path.with_suffix('').with_suffix('.json')
            if json_file.exists():
                json_file.unlink()
                console.print(f"  [red]Deleted: {json_file.name}[/red]")
    
    if dry_run:
        console.print(f"\n[yellow]This is a DRY RUN. Use --execute to delete files.[/yellow]")
    else:
        console.print(f"\n[green]Dropped {len(to_drop)} duplicate files[/green]")


if __name__ == "__main__":
    app()