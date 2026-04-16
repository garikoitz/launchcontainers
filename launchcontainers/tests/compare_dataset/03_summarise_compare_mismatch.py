"""
summarise_compare.py
--------------------
Read a compare_result CSV (from compare_acq_times.py) and print a
rich per-session summary: mismatches, files missing in csv1, files missing in csv2.

Usage
-----
    python summarise_compare.py --input compare_result.txt
"""

import csv
from collections import defaultdict
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

console = Console()
app = typer.Typer(pretty_exceptions_show_locals=False)


def _write_subses_list(path: Path, keys: list[tuple[str, str]]) -> None:
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["sub", "ses", "RUN"])
        for sub, ses in sorted(keys):
            w.writerow([sub, ses, "True"])


@app.command()
def main(
    input: Path = typer.Option(
        ..., "--input", "-i", help="CSV produced by compare_acq_times.py"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Also list individual filenames."
    ),
    outdir: Path = typer.Option(
        Path("."), "--outdir", "-o", help="Directory for the 3 subseslist CSVs."
    ),
    modality: str = typer.Option(
        "func",
        "--modality",
        "-m",
        help="Filter by modality (e.g. func, anat, dwi, fmap). Use 'all' for no filter.",
    ),
):
    mismatches: dict[tuple, list[str]] = defaultdict(list)
    missing_csv1: dict[tuple, list[str]] = defaultdict(list)
    missing_csv2: dict[tuple, list[str]] = defaultdict(list)

    csv1_label = csv2_label = ""

    if modality != "all":
        console.print(f"[dim]filtering by modality: [bold]{modality}[/][/]")

    with open(input, newline="") as fh:
        for row in csv.DictReader(fh):
            if modality != "all" and row.get("modality", "") != modality:
                continue
            key = (row["sub"].zfill(2), row["ses"].zfill(2))
            issue = row["issue"]
            nii = row["nii_name"]

            if issue == "acq_time_mismatch":
                mismatches[key].append(nii)
            elif issue.startswith("missing_in_"):
                label = issue.replace("missing_in_", "").strip(".csv")
                if not csv1_label:
                    csv1_label = label
                if label == csv1_label:
                    missing_csv1[key].append(nii)
                else:
                    if not csv2_label:
                        csv2_label = label
                    missing_csv2[key].append(nii)

    csv1_label = csv1_label or "csv1"
    csv2_label = csv2_label or "csv2"

    total_ses = len(set(mismatches) | set(missing_csv1) | set(missing_csv2))
    console.print(
        f"\n[bold cyan]### Acquisition-time comparison summary[/]  "
        f"affected sessions: [bold]{total_ses}[/]"
    )

    # --- mismatch table ---
    if mismatches:
        console.print(
            f"\n[bold red]Mismatched acq_time ({len(mismatches)} session(s))[/]"
        )
        t = Table(show_header=True, header_style="bold magenta")
        t.add_column("sub")
        t.add_column("ses")
        t.add_column("files", justify="right")
        for (sub, ses), files in sorted(mismatches.items()):
            t.add_row(sub, ses, str(len(files)))
        console.print(t)
        if verbose:
            for (sub, ses), files in sorted(mismatches.items()):
                console.print(
                    f"\n  [bold]sub-{sub}  ses-{ses}[/]  ({len(files)} file(s)):"
                )
                for f in files:
                    console.print(f"    [dim]{f}[/]")

    # --- missing in csv1 ---
    if missing_csv1:
        console.print(
            f"\n[bold yellow]missing in {csv1_label} ({len(missing_csv1)} session(s))[/]"
        )
        t = Table(show_header=True, header_style="bold magenta")
        t.add_column("sub")
        t.add_column("ses")
        t.add_column("files", justify="right")
        for (sub, ses), files in sorted(missing_csv1.items()):
            t.add_row(sub, ses, str(len(files)))
        console.print(t)
        if verbose:
            for (sub, ses), files in sorted(missing_csv1.items()):
                console.print(
                    f"\n  [bold]sub-{sub}  ses-{ses}[/]  ({len(files)} file(s)):"
                )
                for f in files:
                    console.print(f"    [dim]{f}[/]")

    # --- missing in csv2 ---
    if missing_csv2:
        console.print(
            f"\n[bold yellow]missing in {csv2_label} ({len(missing_csv2)} session(s))[/]"
        )
        t = Table(show_header=True, header_style="bold magenta")
        t.add_column("sub")
        t.add_column("ses")
        t.add_column("files", justify="right")
        for (sub, ses), files in sorted(missing_csv2.items()):
            t.add_row(sub, ses, str(len(files)))
        console.print(t)
        if verbose:
            for (sub, ses), files in sorted(missing_csv2.items()):
                console.print(
                    f"\n  [bold]sub-{sub}  ses-{ses}[/]  ({len(files)} file(s)):"
                )
                for f in files:
                    console.print(f"    [dim]{f}[/]")

    if not mismatches and not missing_csv1 and not missing_csv2:
        console.print("[green]No issues found. All acquisition times match.[/]")
        return

    # --- write subseslists ---
    outdir.mkdir(parents=True, exist_ok=True)
    out_mismatch = outdir / "subseslist_mismatch.csv"
    out_missing1 = outdir / f"subseslist_missing_in_{csv1_label}.csv"
    out_missing2 = outdir / f"subseslist_missing_in_{csv2_label}.csv"

    _write_subses_list(out_mismatch, list(mismatches.keys()))
    _write_subses_list(out_missing1, list(missing_csv1.keys()))
    _write_subses_list(out_missing2, list(missing_csv2.keys()))

    console.print("\n[dim]subseslists written:[/]")
    console.print(f"  [dim]{out_mismatch}[/]  ({len(mismatches)} session(s))")
    console.print(f"  [dim]{out_missing1}[/]  ({len(missing_csv1)} session(s))")
    console.print(f"  [dim]{out_missing2}[/]  ({len(missing_csv2)} session(s))")


if __name__ == "__main__":
    app()
