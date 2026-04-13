"""
compare_subseslist.py
---------------------
Summarise, union, or subtract subseslists.

Modes
-----
Inspect one or more inputs (union of all -i, RUN=True only)::

    python compare_subseslist.py -i list1.tsv -i list2.tsv

Compare union of inputs against a reference::

    python compare_subseslist.py -i list1.tsv -r ref.tsv

Subtract exclude lists from ref (ref minus -e files)::

    python compare_subseslist.py -r ref.tsv -e done1.tsv -e done2.tsv

All together — union of inputs, subtract excludes, compare to ref::

    python compare_subseslist.py -r ref.tsv -i list1.tsv -e done.tsv
"""

import csv
import typer
from pathlib import Path
from typing import List, Optional

app = typer.Typer(pretty_exceptions_show_locals=False)


def _read_pairs(path: Path, run_true_only: bool = False) -> set[tuple[str, str]]:
    pairs = set()
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh, delimiter=","):
            if run_true_only and str(row.get("RUN", "")).strip() != "True":
                continue
            sub = str(row["sub"]).strip().zfill(2)
            ses = str(row["ses"]).strip().zfill(2)
            pairs.add((sub, ses))
    return pairs


def _print_pairs(pairs: set[tuple[str, str]]) -> None:
    for sub, ses in sorted(pairs):
        print(f"  {sub},{ses},True")


@app.command()
def main(
    inputs: List[Path] = typer.Option(
        [], "--input", "-i", help="Include list(s); RUN=True only. Union of all -i."
    ),
    excludes: List[Path] = typer.Option(
        [],
        "--exclude",
        "-e",
        help="Exclude list(s); subtracted from ref or input union.",
    ),
    ref: Optional[Path] = typer.Option(
        None, "--ref", "-r", help="Reference subseslist (all rows counted)."
    ),
):
    # --- build input union ---
    input_pairs: set[tuple[str, str]] = set()
    for p in inputs:
        chunk = _read_pairs(p, run_true_only=True)
        print(f"  -i {p.name}: {len(chunk)} session(s) with RUN=True")
        input_pairs |= chunk

    if inputs:
        print(f"  union of inputs : {len(input_pairs)} session(s)")

    # --- build exclude union ---
    exclude_pairs: set[tuple[str, str]] = set()
    for p in excludes:
        chunk = _read_pairs(p, run_true_only=True)
        print(f"  -e {p.name}: {len(chunk)} session(s) in file")
        exclude_pairs |= chunk

    # --- no ref: just report input union (minus excludes) ---
    if ref is None:
        actually_excluded = exclude_pairs & input_pairs
        result = input_pairs - exclude_pairs
        if exclude_pairs:
            print(f"  actually excluded ({len(actually_excluded)}):")
            for sub, ses in sorted(actually_excluded):
                print(f"    sub-{sub}  ses-{ses}")
            print(f"  after exclusion : {len(result)} session(s)")
        print(f"\nsub and sessions ({len(result)}):")
        _print_pairs(result)
        return

    # --- ref mode ---
    ref_pairs = _read_pairs(ref, run_true_only=False)
    print(f"  ref             : {len(ref_pairs)} session(s)")

    # subtract excludes from ref
    ref_after_exclude = ref_pairs - exclude_pairs
    if exclude_pairs:
        actually_excluded = exclude_pairs & ref_pairs
        print(f"  actually excluded ({len(actually_excluded)}):")
        for sub, ses in sorted(actually_excluded):
            print(f"    sub-{sub}  ses-{ses}")
        print(f"  ref after -e    : {len(ref_after_exclude)} session(s)")

    # compare input union vs (ref minus excludes)
    if inputs:
        compare_base = ref_after_exclude
        compare_input = input_pairs
        label = "input union"
    else:
        # no -i given: just show what remains in ref after exclusions
        print(f"\nremaining in ref after exclusion ({len(ref_after_exclude)}):")
        _print_pairs(ref_after_exclude)
        return

    missing = sorted(compare_base - compare_input)
    extra = sorted(compare_input - compare_base)

    print()
    if missing:
        print(f"in ref (after -e) but MISSING from {label} ({len(missing)}):")
        for sub, ses in missing:
            print(f"  sub-{sub}  ses-{ses}")
    else:
        print(f"no sessions missing from {label} ✓")

    if extra:
        print(f"\nin {label} but NOT in ref ({len(extra)}):")
        for sub, ses in extra:
            print(f"  sub-{sub}  ses-{ses}")


if __name__ == "__main__":
    app()
