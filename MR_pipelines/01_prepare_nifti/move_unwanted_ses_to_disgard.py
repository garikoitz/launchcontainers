import os
import shutil
from pathlib import Path


def find_match(PROJECT_ROOT, TARGET):
    matches = []
    for root, dirs, files in os.walk(PROJECT_ROOT):
        p = Path(root)
        # Check if the path ends with "sub-01/ses-08"
        if p.as_posix().endswith(TARGET.as_posix()):
            matches.append(p)

    # Dry run: review
    for p in matches:
        print("FOUND:", p)

    return matches
    
################################################################################
def move_unwanted_ses_to_disgard(TARGET):
    PROJECT_ROOT = Path("/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS")
    QUARANTINE   = Path("/bcbl/home/public/Gari/VOTCLOC/discarded/BIDS")

    matches = find_match(PROJECT_ROOT, TARGET)

    # Move while preserving relative paths
    for src in matches:
        rel = src.relative_to(PROJECT_ROOT)   # e.g., derivatives/fmriprep/sub-01/ses-08
        dst = QUARANTINE / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))
        print(f"MOVED: {src} -> {dst}")

TARGET       = Path("sub-06/ses-10")  # the subject-session to move
move_unwanted_ses_to_disgard(TARGET)

################################################################################


def rename_ses_to_targ(SRC, TARGET):
    """
    This function is used to rename all the folder from src to target and also the bids componment under it
    """
    PROJECT_ROOT = Path("/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS")

    matches = find_match(PROJECT_ROOT, SRC)


    renamed=[]
    # Move while preserving relative paths
    for src in matches:
        rel = src.relative_to(PROJECT_ROOT)   # e.g., derivatives/fmriprep/sub-01/ses-08
        new_path = PROJECT_ROOT / rel.parent / TARGET.name
        src.rename(new_path)
        renamed.append((src, new_path))
    # Report what happened
    for old, new in renamed:
        print(f"RENAMED: {old} -> {new}")

    def rename_content_under_path(path):
        if src.name in path.name:
            path.rename(path.with_name(path.name.replace(src.name, new_path.name)))

    
    for src,new_path in renamed:
        for path in new_path.iterdir():
            # if the things under new_path have sub-/ses- info, rename the ses part
            rename_content_under_path(path)
            # if the things under new_path is a dir, do it for the next dir
            if path.is_dir():
                # judge for the sub dir
                for sub_p in path.iterdir():
                    rename_content_under_path(sub_p)
                    if sub_p.is_dir():
                        for subb_p in subp.iterdir():
                            rename_content_under_path(subb_p)

# rename subses from orig to targ
SRC       = Path("sub-11/ses-02rerun")  
TARGET    = Path("sub-11/ses-02")
rename_ses_to_targ(SRC, TARGET)