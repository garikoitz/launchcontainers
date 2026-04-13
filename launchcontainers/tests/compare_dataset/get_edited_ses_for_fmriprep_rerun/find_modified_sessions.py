"""
find_modified_sessions.py
-------------------------
Find sub/ses sessions that have at least one func/*sbref* or func/*bold*
file modified in a given year/month under a BIDS dir.
Outputs a CSV with columns: sub, ses, RUN.

Usage
-----
    python find_modified_sessions.py --bidsdir /path/to/BIDS
    python find_modified_sessions.py --bidsdir /path/to/BIDS --year 2026 --month 3
    python find_modified_sessions.py --bidsdir /path/to/BIDS --output modified.csv
    python find_modified_sessions.py --bidsdir /path/to/BIDS --workers 40
"""

import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

console = Console()
app = typer.Typer(pretty_exceptions_show_locals=False)


def _check_session(ses_dir: Path, year: int, month: int) -> dict | None:
    """Return row dict if any func/*sbref* or func/*bold* file was modified in year/month."""
    func_dir = ses_dir / "func"
    if not func_dir.is_dir():
        return None

    matched_files: list[tuple[str, str]] = []  # (filename, mtime_str)
    for f in sorted(func_dir.iterdir()):
        if not f.is_file():
            continue
        name = f.name
        if "sbref" not in name and "bold" not in name:
            continue
        mtime = datetime.fromtimestamp(f.stat().st_mtime)
        if mtime.year == year and mtime.month == month:
            matched_files.append((name, mtime.strftime("%Y-%m-%d %H:%M:%S")))

    if not matched_files:
        return None

    latest_mtime = max(t for _, t in matched_files)
    return {
        "sub": ses_dir.parent.name.replace("sub-", ""),
        "ses": ses_dir.name.replace("ses-", ""),
        "modified": latest_mtime,
        "files": matched_files,
    }


@app.command()
def main(
    bidsdir: Path = typer.Option(..., "--bidsdir", "-b"),
    year: int = typer.Option(2026, "--year", "-y", help="Year to filter."),
    month: int = typer.Option(3, "--month", "-m", help="Month to filter (1-12)."),
    output: Optional[Path] = typer.Option(
        None, "--output", "-o", help="Output CSV. Default: print only."
    ),
    workers: int = typer.Option(40, "--workers", "-w", help="Parallel workers."),
):
    """Find sessions with func/bold or func/sbref files modified in year/month."""
    ses_dirs = sorted(d for d in Path(bidsdir).glob("sub-*/ses-*") if d.is_dir())
    console.print(
        f"[dim]Scanning {len(ses_dirs)} session dirs with {workers} workers…[/]"
    )

    rows = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_check_session, d, year, month): d for d in ses_dirs}
        for fut in as_completed(futures):
            result = fut.result()
            if result is not None:
                rows.append(result)

    rows.sort(key=lambda r: (r["sub"], r["ses"]))

    total_files = sum(len(r["files"]) for r in rows)
    console.print(
        f"\n[bold cyan]{len(rows)} session(s) with modified func/bold or func/sbref in {year}-{month:02d} "
        f"— {total_files} file(s) total[/]"
    )

    # Summary table
    t = Table(show_header=True, header_style="bold magenta")
    t.add_column("sub")
    t.add_column("ses")
    t.add_column("files", justify="right")
    t.add_column("latest modified")
    for r in rows:
        t.add_row(r["sub"], r["ses"], str(len(r["files"])), r["modified"])
    console.print(t)

    # Detailed log per session
    console.print("\n[bold]Detailed file log:[/bold]")
    for r in rows:
        console.print(
            f"\n  [cyan]sub-{r['sub']}  ses-{r['ses']}[/cyan]  ({len(r['files'])} file(s)):"
        )
        for fname, mtime in r["files"]:
            console.print(f"    [dim]{mtime}[/dim]  {fname}")

    if output:
        with open(output, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["sub", "ses", "RUN"])
            w.writeheader()
            for r in rows:
                w.writerow({"sub": r["sub"], "ses": r["ses"], "RUN": "True"})
        console.print(f"[dim]→ {output}[/]")


if __name__ == "__main__":
    app()
