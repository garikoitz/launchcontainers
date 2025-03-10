from __future__ import annotations

from pathlib import Path

from bids import BIDSLayout


basedir = '/bcbl/home/public/Gari/main_exp/VOTCLOC'
bids_dir_name = 'BIDS'
fmriprep_bidslayout = True
fmriprep_analysis_name = 'runall_US'

bids_dir = Path(basedir) / bids_dir_name

if fmriprep_bidslayout:
    fmriprep_dir = Path('derivatives') / f"fmriprep-{fmriprep_analysis_name}"
else:
    fmriprep_dir = (
        Path('derivatives') / 'fmriprep' / f"analysis-{fmriprep_analysis_name}"
    )


layout = BIDSLayout(bids_dir)
