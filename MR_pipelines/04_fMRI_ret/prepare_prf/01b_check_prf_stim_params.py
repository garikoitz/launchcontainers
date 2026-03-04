import re
import csv
from pathlib import Path
from glob import glob
from os import path
import numpy as np
from scipy.io import loadmat


def parse_stim_params(stim_name: str) -> tuple[str, str, str]:
    """Extract flickfreq, barWidth, letsize from stimName path string."""
    flickfreq = re.search(r'flickfreq-([\d.]+)Hz', stim_name)
    barwidth   = re.search(r'barWidth-([\d.]+)deg', stim_name)
    letsize    = re.search(r'letsize-([\d.]+)',      stim_name)
    return (
        flickfreq.group(1) if flickfreq else "?",
        barwidth.group(1)  if barwidth  else "?",
        letsize.group(1)   if letsize   else "?",
    )

def parse_stim_params(stim_name: str) -> tuple[str, str, str]:
    """Extract flickfreq, barWidth, letsize from stimName path string."""
    flickfreq = re.search(r'flickfreq-([\d.]+)Hz', stim_name)
    barwidth   = re.search(r'barWidth-([\d.]+)deg', stim_name)
    letsize    = re.search(r'letsize-([\d.]+)',      stim_name)
    return (
        flickfreq.group(1) if flickfreq else "?",
        barwidth.group(1)  if barwidth  else "?",
        letsize.group(1)   if letsize   else "?",
    )


def make_summary(stim_name: str, flick: str, bar: str, let: str) -> str:
    if "fix" in Path(stim_name).name.lower():
        return "wordcenter"
    return f"{flick}-{bar}-{let}"


EXCLUDED_SESSIONS: set[tuple[str, str]] = {}

def default_combinations() -> list[tuple[str, str]]:
    """All 11 subs × 10 sessions minus excluded."""
    combos = []
    for sub_id in range(1, 12):
        for ses_id in range(1, 11):
            sub = f"sub-{sub_id:02d}"
            ses = f"ses-{ses_id:02d}"
            if (sub, ses) not in EXCLUDED_SESSIONS:
                combos.append((sub, ses))
    return combos


if __name__ == "__main__":
    sourcedata = "/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS/sourcedata"
    output_csv = Path(sourcedata) / "stim_params_summary.csv"

    all_rows = []
    for sub_str, ses_str in default_combinations():
        sub = sub_str.replace("sub-", "")
        ses = ses_str.replace("ses-", "")

        mat_files = np.sort(glob(path.join(sourcedata, f"sub-{sub}", f"ses-{ses}", "20*.mat")))

        if mat_files.size == 0:
            print(f"[SKIP] sub-{sub} ses-{ses} — no .mat files")
            continue

        # Take first mat file only — one combination per session
        matfile = mat_files[0]
        if mat_files.size > 1:
            print(f"[WARN] sub-{sub} ses-{ses} — {mat_files.size} .mat files found, using first: {Path(matfile).name}")

        try:
            stim_name = loadmat(matfile, simplify_cells=True)["params"]["loadMatrix"]
            flick, bar, let = parse_stim_params(stim_name)
        except Exception as e:
            print(f"[WARN] sub-{sub} ses-{ses} — could not read {Path(matfile).name}: {e}")
            flick, bar, let = "?", "?", "?"


        summary = make_summary(stim_name, flick, bar, let)
        print(f"  sub-{sub} ses-{ses} → {summary}")
        all_rows.append({
            "sub":       sub,
            "ses":       ses,
            "matfile":   Path(matfile).name,
            "flickfreq": flick,
            "barWidth":  bar,
            "letsize":   let,
            "summary":   summary,
        })

    # Write single CSV for all sessions
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["sub", "ses", "matfile", "flickfreq", "barWidth", "letsize", "summary"]
        )
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nDone. {len(all_rows)}/110 sessions written to {output_csv}")