import os
from pathlib import Path

bids_dir = Path("/scratch/tlei/old_VOTCLOC/BIDS")

# Loop over all subject/session folders (exclude derivatives)
for sub_dir in bids_dir.glob("sub-*"):
    if sub_dir.name == "derivatives":
        print('skip')
        continue  # just in case

    for ses_dir in sub_dir.glob("ses-*"):
        for root, dirs, files in os.walk(ses_dir):
            for name in files:
                # don't unlink event files
                if not 'events' in name:
                    path = Path(root) / name
                    if path.is_symlink():
                        print(f"Deleting symbolic link: {path}")
                        path.unlink()

            # (Optional) also remove symbolic-linked directories
            for name in dirs:
                dpath = Path(root) / name
                if dpath.is_symlink():
                    print(f"Deleting symbolic link directory: {dpath}")
                    dpath.unlink()
