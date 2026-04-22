"""
qc_glm.py
---------
Check whether GLM output directories exist (and are non-empty) for each
sub/ses under a target folder.

Expected layout::

    <target_dir>/sub-XX/ses-XX/

Usage
-----
Check a single session::

    python qc_glm.py -d /path/to/l1_surface/analysis-v2 -s 09,06

Check all sessions in a subseslist file::

    python qc_glm.py -d /path/to/l1_surface/analysis-v2 -f subseslist.txt

Output
------
A summary table is printed to stdout.  Sessions whose directory is missing
or empty are written as a comma-separated subseslist to
``<target_dir>_missing.txt`` (or the path given by ``--output``).
"""

from __future__ import annotations

import csv
import os
import os.path as op
from pathlib import Path
from typing import List, Optional

import typer

app = typer.Typer(pretty_exceptions_show_locals=False)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_pairs(path: Path) -> list[tuple[str, str]]:
    """Read sub/ses pairs from a comma-separated subseslist file.

    Accepts files with or without a header row containing 'sub' and 'ses'
    columns.  The RUN column (if present) is ignored — all rows are included.
    """
    pairs: list[tuple[str, str]] = []
    with open(path, newline="") as fh:
        sample = fh.read(1024)
        fh.seek(0)
        has_header = "sub" in sample.lower().split("\n")[0]
        reader = csv.DictReader(fh) if has_header else csv.reader(fh)
        for row in reader:
            if has_header:
                sub = str(row["sub"]).strip().zfill(2)
                ses = str(row["ses"]).strip().zfill(2)
            else:
                parts = [c.strip() for c in row]
                sub = parts[0].zfill(2)
                ses = parts[1].zfill(2)
            pairs.append((sub, ses))
    return pairs


def _is_empty_dir(path: str) -> bool:
    """Return True if the directory exists but contains no files (recursively)."""
    for _root, _dirs, files in os.walk(path):
        if files:
            return False
    return True


def _check_session(target_dir: str, sub: str, ses: str) -> str:
    """Return status string: 'ok', 'missing', or 'empty'."""
    ses_dir = op.join(target_dir, f"sub-{sub}", f"ses-{ses}")
    if not op.isdir(ses_dir):
        return "missing"
    if _is_empty_dir(ses_dir):
        return "empty"
    return "ok"


# ---------------------------------------------------------------------------
# Main command
# ---------------------------------------------------------------------------

@app.command()
def main(
    target_dir: Path = typer.Option(
        ..., "--dir", "-d", help="Root output directory to check (contains sub-XX/ses-XX/)."
    ),
    subses_arg: Optional[str] = typer.Option(
        None, "--subses", "-s", help="Single sub,ses pair, e.g. 09,06"
    ),
    file_arg: Optional[Path] = typer.Option(
        None, "--file", "-f", help="Path to subseslist file (CSV with sub,ses columns)."
    ),
    output: Optional[Path] = typer.Option(
        None, "--output", "-o",
        help="Where to write the missing-sessions subseslist. "
             "Default: <target_dir>_missing.txt next to the target directory."
    ),
):
    """Check GLM output directories for each sub/ses and report missing/empty ones."""

    if subses_arg is None and file_arg is None:
        typer.echo("Error: provide -s <sub>,<ses>  or  -f <subseslist>", err=True)
        raise typer.Exit(1)

    # Build pairs list
    pairs: list[tuple[str, str]] = []
    if subses_arg is not None:
        parts = subses_arg.split(",")
        pairs.append((parts[0].strip().zfill(2), parts[1].strip().zfill(2)))
    else:
        if not file_arg.exists():
            typer.echo(f"Error: file not found: {file_arg}", err=True)
            raise typer.Exit(1)
        pairs = _read_pairs(file_arg)

    target = str(target_dir)
    typer.echo(f"\nTarget dir : {target}")
    typer.echo(f"Sessions   : {len(pairs)}\n")

    # Check each session
    results: list[tuple[str, str, str]] = []
    for sub, ses in pairs:
        status = _check_session(target, sub, ses)
        results.append((sub, ses, status))

    # Print summary table
    col_w = 10
    header = f"{'sub':>{col_w}}  {'ses':>{col_w}}  {'status'}"
    typer.echo(header)
    typer.echo("-" * len(header))
    for sub, ses, status in results:
        icon = "✓" if status == "ok" else "✗"
        typer.echo(f"  sub-{sub}  ses-{ses}  {icon}  {status}")

    # Summary counts
    n_ok      = sum(1 for _, _, s in results if s == "ok")
    n_missing = sum(1 for _, _, s in results if s == "missing")
    n_empty   = sum(1 for _, _, s in results if s == "empty")

    typer.echo(f"\n  ok={n_ok}  missing={n_missing}  empty={n_empty}  total={len(results)}")

    # Write subseslist of failed sessions
    failed = [(sub, ses) for sub, ses, s in results if s != "ok"]
    if failed:
        if output is None:
            out_path = Path(str(target_dir).rstrip("/") + "_missing.txt")
        else:
            out_path = output

        with open(out_path, "w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["sub", "ses", "RUN"])
            for sub, ses in failed:
                writer.writerow([sub, ses, "True"])

        typer.echo(f"\n  Missing/empty sessions written → {out_path}")
    else:
        typer.echo("\n  All sessions present and non-empty. No output file written.")


if __name__ == "__main__":
    app()
