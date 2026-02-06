import pandas as pd
from bids import BIDSLayout
from pathlib import Path

from datetime import datetime, timedelta

def times_match(t1, t2, max_diff_sec=30):
    """
    Compare two BIDS-style AcquisitionTime strings.
    
    Parameters
    ----------
    t1 : str
        AcquisitionTime string, e.g. "14:04:15.487500"
    t2 : str
        AcquisitionTime string, e.g. "14:04:29.487500"
    max_diff_sec : int
        Maximum allowed difference in seconds to consider times 'matching'.
    
    Returns
    -------
    bool
        True if |t1 - t2| <= max_diff_sec
    """
    if t1 is None or t2 is None:
        return False
    
    # Handle strings like HH:MM:SS or HH:MM:SS.ssssss
    fmt = "%H:%M:%S.%f" if "." in t1 else "%H:%M:%S"
    
    dt1 = datetime.strptime(t1, fmt)
    dt2 = datetime.strptime(t2, fmt)
    
    diff = abs((dt1 - dt2).total_seconds())
    
    return diff <= max_diff_sec

def extract_bids_qc(bids_root):
    layout = BIDSLayout(bids_root, validate=False)

    records = []

    # Query all relevant modalities
    files = layout.get(
        suffix=['T1w', 'T2w', 'epi','dwi', 'bold'],
        extension=['.nii', '.nii.gz']
    )

    # Helper: find corresponding sbref for a bold
    def find_sbref(f):
        matches = layout.get(
            subject=f.entities.get("subject"),
            session=f.entities.get("session"),
            task=f.entities.get("task"),
            run=f.entities.get("run"),
            acquisition=f.entities.get("acq"),
            direction=f.entities.get("direction"),
            suffix="sbref",
            extension=['.nii', '.nii.gz']
        )
        return matches[0] if len(matches) > 0 else None

    for f in files:
        suffix = f.entities.get("suffix")
        md = f.get_metadata() or {}

        # Pair BOLD → SBREF
        if suffix == "bold":
            f_sbref = find_sbref(f)
            bold_time = md.get("AcquisitionTime")
            sbref_time = f_sbref.get_metadata().get("AcquisitionTime") if f_sbref else None
            sbref_match = times_match(bold_time, sbref_time)
        record = {
            "sub": f.entities.get("subject"),
            "ses": f.entities.get("session"),
            "modality": suffix,
            "run": f.entities.get("run"),
            "task": f.entities.get("task"),
            "acq": f.entities.get("acq"),
            "dir": f.entities.get("direction"),
            "filepath": f.path,
            "sbref": sbref_match if suffix == "bold" else None,
            "AcquisitionDate": md.get("AcquisitionDate"),
            "AcquisitionTime": md.get("AcquisitionTime"),
        }



        records.append(record)

    # Make DataFrame
    df = pd.DataFrame(records)

    # Sort for readability
    df = df.sort_values(
        ["sub", "ses", "modality", "task", "run"],
        na_position="last"
    )

    return df

def _safe_int_convert(series):
    """
    Convert a pandas Series to integer where possible.
    Non-convertible values become NaN (float).
    """
    return pd.to_numeric(series, errors="coerce").astype("Int64")

if __name__ == "__main__":
    bids_dir = Path("/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS")
    df = extract_bids_qc(bids_dir)
    output_dir = bids_dir / "qc_reports.tsv"
    df.to_csv("bids_conversion_qc.tsv", sep="\t", index=False)
    df["sub"] = _safe_int_convert(df["sub"])
    df["ses"] = _safe_int_convert(df["ses"])
    df["run"] = _safe_int_convert(df["run"])

    print(df)