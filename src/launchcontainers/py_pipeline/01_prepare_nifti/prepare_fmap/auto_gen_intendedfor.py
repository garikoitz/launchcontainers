import os.path as op
import os 
from bids import BIDSLayout
import json
from heudiconv import bids as hb
import pandas as pd

def remove_intended_for(json_path: str, force):
    """
    Removes the 'IntendedFor' field from a BIDS JSON file if it exists.

    Parameters:
    -----------
    json_path : str
        Path to the JSON file.

    Returns:
    --------
    None
        The function modifies the file in place, saving it without 'IntendedFor'.
    """
    try:
        # Load the JSON file
        with open(json_path, "r") as f:
            data = json.load(f)

        # Check if 'IntendedFor' exists and remove it
        if "IntendedFor" in data and force:
            del data["IntendedFor"]
            print(f"Removed 'IntendedFor' from {json_path}")

            # Save the modified JSON file
            with open(json_path, "w") as f:
                json.dump(data, f, indent=4)
        else:
            print(f"'IntendedFor' not found in {json_path}")

    except FileNotFoundError:
        print(f"Error: File not found - {json_path}")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format - {json_path}")

matching_parameters=[
    "Shims",
    "ImagingVolume",
    "ModalityAcquisitionLabel",
    "CustomAcquisitionLabel",
    "PlainAcquisitionLabel",
    "Force",
]

criteria=['First', 'Closest']



basedir='/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS'
code_dir='/export/home/tlei/tlei/soft/MRIworkflow/01_prepare_nifti/prepare_fmap'
subseslist= pd.read_csv(os.path.join(code_dir,"subses_List.txt"),sep='\t', dtype='str')
force=True
layout=BIDSLayout(basedir)
for row in subseslist.itertuples(index=False):
    sub = row.sub
    ses = row.ses
    subsesdir=layout.get(subject=sub, session=ses, suffix='scans')[0].dirname
    hb.populate_intended_for(subsesdir, "Shims" ,'First')

