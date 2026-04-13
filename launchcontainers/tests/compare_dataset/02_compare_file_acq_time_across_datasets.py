"""
compare_acq_times.py
--------------------
Compare AcquisitionTime across two CSVs produced by scan_acq_times.py.
For each nii_name, check if acq_time matches. Report mismatches per session.

Usage
-----
    python compare_acq_times.py --csv1 dir1.csv --csv2 dir2.csv
    python compare_acq_times.py --csv1 dir1.csv --csv2 dir2.csv --output mismatches.csv
"""

import csv
from collections import defaultdict
from pathlib import Path
from typing import Optional
import typer

app = typer.Typer(pretty_exceptions_show_locals=False)


def _load(path: Path) -> dict[str, dict]:
    """Return {nii_name: row} from a CSV."""
    data = {}
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            data[row["nii_name"]] = row
    return data


@app.command()
def main(
    csv1: Path = typer.Option(..., "--csv1"),
    csv2: Path = typer.Option(..., "--csv2"),
    output: Optional[Path] = typer.Option(
        None, "--output", "-o", help="Write mismatch CSV. Default: print only."
    ),
):
    d1 = _load(csv1)
    d2 = _load(csv2)

    all_names = sorted(n for n in set(d1) | set(d2) if "gfactor" not in n)

    mismatches = []
    only_in_1 = []
    only_in_2 = []

    for name in all_names:
        if name not in d2:
            only_in_1.append(d1[name])
            continue
        if name not in d1:
            only_in_2.append(d2[name])
            continue
        t1 = d1[name]["acq_time"]
        t2 = d2[name]["acq_time"]
        if t1 != t2:
            mismatches.append(
                {
                    "sub": d1[name]["sub"],
                    "ses": d1[name]["ses"],
                    "modality": d1[name].get("modality", ""),
                    "nii_name": name,
                    "acq_time_csv1": t1,
                    "acq_time_csv2": t2,
                }
            )

    # --- summary per session ---
    by_session: dict[tuple, list] = defaultdict(list)
    for m in mismatches:
        by_session[(m["sub"], m["ses"])].append(m["nii_name"])

    total = len(mismatches) + len(only_in_1) + len(only_in_2)
    print(f"\nchecked  : {len(all_names)} files")
    print(f"matched  : {len(all_names) - total}")
    print(f"mismatch : {len(mismatches)}")
    print(f"only in {csv1.name} : {len(only_in_1)}")
    print(f"only in {csv2.name} : {len(only_in_2)}")

    if by_session:
        print("\n--- mismatches by session ---")
        for (sub, ses), files in sorted(by_session.items()):
            print(f"  sub-{sub}  ses-{ses}  ({len(files)} file(s)):")
            for f in files:
                m = next(x for x in mismatches if x["nii_name"] == f)
                print(f"    {f}")
                print(f"      csv1: {m['acq_time_csv1']}")
                print(f"      csv2: {m['acq_time_csv2']}")

    # group missing files by session
    missing_by_session: dict[tuple, list] = defaultdict(list)
    for r in only_in_1:
        missing_by_session[(r["sub"], r["ses"])].append(
            (r["nii_name"], csv1.name, csv2.name)
        )
    for r in only_in_2:
        missing_by_session[(r["sub"], r["ses"])].append(
            (r["nii_name"], csv2.name, csv1.name)
        )

    if missing_by_session:
        print("\n--- missing files by session ---")
        for (sub, ses), files in sorted(missing_by_session.items()):
            print(f"  sub-{sub}  ses-{ses}  ({len(files)} file(s)):")
            for nii, present_in, absent_in in files:
                print(f"    {nii}")
                print(f"      present in : {present_in}")
                print(f"      missing in : {absent_in}")

    if output:
        missing_rows = [
            {
                "sub": r["sub"],
                "ses": r["ses"],
                "modality": r.get("modality", ""),
                "nii_name": r["nii_name"],
                "acq_time_csv1": r["acq_time"],
                "acq_time_csv2": "",
                "issue": f"missing_in_{csv2.name}",
            }
            for r in only_in_1
        ] + [
            {
                "sub": r["sub"],
                "ses": r["ses"],
                "modality": r.get("modality", ""),
                "nii_name": r["nii_name"],
                "acq_time_csv1": "",
                "acq_time_csv2": r["acq_time"],
                "issue": f"missing_in_{csv1.name}",
            }
            for r in only_in_2
        ]
        mismatch_rows = [{**m, "issue": "acq_time_mismatch"} for m in mismatches]
        all_rows = mismatch_rows + missing_rows
        if all_rows:
            with open(output, "w", newline="") as fh:
                w = csv.DictWriter(
                    fh,
                    fieldnames=[
                        "sub",
                        "ses",
                        "modality",
                        "nii_name",
                        "acq_time_csv1",
                        "acq_time_csv2",
                        "issue",
                    ],
                )
                w.writeheader()
                w.writerows(all_rows)
            print(f"\nreport ({len(all_rows)} rows) → {output}")


if __name__ == "__main__":
    app()
