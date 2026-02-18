from pathlib import Path
import nibabel as nib
import numpy as np

bids_dir = Path('/bcbl/home/public/Gari/VOTCLOC/main_exp/raw_nifti/sub-11/ses-10/dwi')

for mag in bids_dir.rglob("*magnitude.nii.gz"):
    if 'orig' in mag.name:
        continue
    orig = mag.parent / mag.name.replace('magnitude.nii.gz', 'magnitude_orig.nii.gz')
    if orig.exists():
        same = np.allclose(nib.load(mag).get_fdata(), nib.load(orig).get_fdata())
        print(f"{'✓' if same else '✗'} {mag.name}: {'same' if same else 'DIFFERENT'}")