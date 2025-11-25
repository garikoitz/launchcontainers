'''
This code will check T2w image under raw_nifti dir and sync it to BIDS dir
'''
import os
from bids.layout import BIDSLayout
import shutil
# use pybids to get layout for both dir
basedir = '/bcbl/home/public/Gari/VOTCLOC/main_exp'

raw_nifti_dir = os.path.join(basedir, 'raw_nifti')
bids_dir = os.path.join(basedir, 'BIDS')

rl = BIDSLayout(raw_nifti_dir, derivatives=False, validate=False)
bl = BIDSLayout(bids_dir, derivatives=False, validate=False)
# ask for a certain sub and ses
subs=['09']
sess=['01']

def copy_t2(sub,ses,rl,bl):
    # get the raw_nifti T2, and get the BIDS T1
    src_t2=rl.get(subject=sub,session=ses,suffix='T2w')
    print(src_t2)
    targ_t2=bl.get(subject=sub,session=ses,suffix='T2w')
    skipped = []
    # if target T2 doesn't exist, and src_T2 length is 2 copy the src T2 to targ T2
    if not targ_t2:
        # copy the src_t2 to targ t2
        src_t2_path = [i.path for i in src_t2]
        targ_t2_path = [i.replace("raw_nifti","BIDS") for i in src_t2_path]

        for src, targ in zip(src_t2_path, targ_t2_path):
            if os.path.exists(targ):
                skipped.append(os.path.basename(targ))
                continue
            os.makedirs(os.path.dirname(targ), exist_ok=True)
            try:
                shutil.copy(src, targ)
                print(f">>>>>>>>>>>>> success {src.split('/')[-1]} ")
            except:
                print(f"#### FAILED {src.split('/')[-1]} #####")

# else print that src doesn't exist
for sub in subs:
    for ses in sess:
        copy_t2(sub,ses,rl,bl)