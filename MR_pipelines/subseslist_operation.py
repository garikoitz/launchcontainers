#!/usr/bin/env python3
"""
Combine multiple incomplete.csv/.txt files and split into keep/delete lists.

Usage:
    python combine_subseslist.py file1.csv file2.csv -o output_dir -p myprefix
    python combine_subseslist.py *.csv -o ./qc -p combined
"""
import csv
from collections import defaultdict
from pathlib import Path
from typing import Optional

import typer

app = typer.Typer()


@app.command()
def combine(
    input_files: list[Path] = typer.Argument(..., help="Input CSV/TXT files (comma-separated, with sub,ses,RUN columns)."),
    output_dir: Path = typer.Option(Path("."), "--output-dir", "-o", help="Output directory."),
    prefix: str = typer.Option("combined", "--prefix", "-p", help="Output filename prefix."),
) -> None:
    """Combine RUN status across multiple csv/txt files. Keep if all True, delete if any False."""

    run_status: dict[tuple[str, str], list[bool]] = defaultdict(list)
    n_files = 0
    for fpath in input_files:
        if not fpath.exists():
            typer.echo(f"[WARN] File not found, skipping: {fpath}")
            continue

        n_rows = 0
        with open(fpath) as f:
            reader = csv.DictReader(f, delimiter=",")
            for row in reader:
                sub = row["sub"].strip().replace("sub-", "")
                ses = row["ses"].strip().replace("ses-", "")
                run = row["RUN"].strip().lower() == "true"
                run_status[(sub, ses)].append(run)
                n_rows += 1
        n_files += 1
        typer.echo(f"  Read {n_rows:3d} rows from {fpath.name}")

    if not run_status:
        typer.echo("[ERROR] No valid rows found across all input files.")
        raise typer.Exit(code=1)

    keep:   list[tuple[str, str]] = []
    delete: list[tuple[str, str]] = []

    for (sub, ses), statuses in sorted(run_status.items()):
        if all(statuses) and len(statuses) == n_files:
            keep.append((sub, ses))
        elif not all(statuses) and len(statuses) == n_files:
            delete.append((sub, ses))

    output_dir.mkdir(parents=True, exist_ok=True)

    keep_path   = output_dir / f"{prefix}_ready.txt"
    delete_path = output_dir / f"{prefix}_need_work.txt"

    with open(keep_path, "w") as f:
        f.write("sub,ses,RUN\n")
        for sub, ses in keep:
            f.write(f"{sub},{ses},True\n")

    with open(delete_path, "w") as f:
        f.write("sub,ses,RUN\n")
        for sub, ses in delete:
            f.write(f"{sub},{ses},True\n")

    typer.echo(f"\nKeep:   {len(keep):3d} sessions → {keep_path}")
    typer.echo(f"Delete: {len(delete):3d} sessions → {delete_path}")


if __name__ == "__main__":
    app()