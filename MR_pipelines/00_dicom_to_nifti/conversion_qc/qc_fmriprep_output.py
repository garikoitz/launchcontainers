import pandas as pd
from bids import BIDSLayout

def extract_fmriprep_qc(fmriprep_root):
    """
    Extract QC records from an fMRIPrep derivatives directory.
    Matches the structure of BIDS QC extraction.
    """

    layout = BIDSLayout(fmriprep_root, validate=False, derivatives=False)

    records = []

    # Query relevant modalities from fMRIPrep
    files = layout.get(
        suffix=['T1w', 'T2w', 'bold', 'epi', 
                'phasediff', 'magnitude1', 'magnitude2'],
        extension=['.nii.gz', '.nii', '.gii']
    )

    for f in files:
        suffix = f.entities.get("suffix")
        md = f.get_metadata() or {}

        record = {
            "sub": f.entities.get("subject"),
            "ses": f.entities.get("session"),
            "modality": suffix,
            "task": f.entities.get("task"),
            "run": f.entities.get("run"),
            "acq": f.entities.get("acq"),
            "dir": f.entities.get("direction"),
            "echo": f.entities.get("echo"),
            "rec": f.entities.get("rec"),
            "space": f.entities.get("space"),
            "hemi": f.entities.get("hemi"),
            "filepath": f.path,
            # pulled from JSON sidecar if present
            "AcquisitionDate": md.get("AcquisitionDate"),
            "AcquisitionTime": md.get("AcquisitionTime"),
            "SeriesDescription": md.get("SeriesDescription"),
            "ProtocolName": md.get("ProtocolName"),
            # no sbref in fMRIPrep outputs
            "sbref": None,
        }

        records.append(record)

    df = pd.DataFrame(records)

    # ----------------------------------------
    # SPECIAL CASE: detect fsnative surface BOLD
    # ----------------------------------------
    # These files look like:
    # sub-01/ses-01/func/sub-01_ses-01_task-XXX_run-01_space-fsnative_hemi-L_bold.func.gii
    # sub-01/ses-01/func/sub-01_ses-01_task-XXX_run-01_space-fsnative_hemi-R_bold.func.gii

    fsnative = layout.get(
        suffix="bold",
        extension=".gii",
        space="fsnative"
    )

    for f in fsnative:
        md = f.get_metadata() or {}

        record = {
            "sub": f.entities.get("subject"),
            "ses": f.entities.get("session"),
            "modality": "bold_fsnative",
            "task": f.entities.get("task"),
            "run": f.entities.get("run"),
            "acq": f.entities.get("acq"),
            "dir": f.entities.get("direction"),
            "echo": f.entities.get("echo"),
            "rec": f.entities.get("rec"),
            "space": f.entities.get("space"),  # fsnative
            "hemi": f.entities.get("hemi"),
            "filepath": f.path,
            "AcquisitionDate": md.get("AcquisitionDate"),
            "AcquisitionTime": md.get("AcquisitionTime"),
            "SeriesDescription": md.get("SeriesDescription"),
            "ProtocolName": md.get("ProtocolName"),
            "sbref": None,
        }

        records.append(record)

    df = pd.DataFrame(records)

    # Sort for readability
    df = df.sort_values(
        ["sub", "ses", "modality", "task", "run", "hemi"],
        na_position="last"
    )

    return df

if __name__ == "__main__":
    fmriprep_dir = Path("/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS/derivatives/fmriprep-25.1.4_t2-fs_dummyscans-5_bold2anat-t2w_forcebbr")
    df = extract_fmriprep_qc(fmriprep_dir)
    output_dir = fmriprep_dir / "qc_fmriprep_reports.tsv"
    df.to_csv("bids_conversion_qc.tsv", sep="\t", index=False)
    print(df)