import os
import subprocess
from bids import BIDSLayout

def sync_bids_component(src_bids_folder, targ_bids_folder):
    """
    Use it once when there is  nothing in the targ bids folder
    """
    bids_folder_componment=[
        "dataset_description.json",
        "participants.json",
        "participants.tsv",
        "README"
    ]
    for item in bids_folder_componment:
        # Ensure destination directory exists
        os.makedirs(targ_bids_folder, exist_ok=True)
        src_item=os.path.join(src_bids_folder,item)
        targ_item=os.path.join(targ_bids_folder,item)

        rsync_command = ["rsync", "-av", src_item, targ_item]
        subprocess.run(rsync_command, check=True)
        print(f"Copied {src_item} to {targ_item}")

def copy_fmap_folders(src_bids_folder, targ_bids_folder, sub=None, ses=None):
    # Initialize BIDS Layout
    layout = BIDSLayout(src_bids_folder)
    if (sub or ses): 
        print(f"Copy fmap folders Working on sub-{sub}, ses-{ses}")
        
        # Get all fmap directories by finding fmap files and extracting unique directories
        fmap_path=[i.dirname for i in layout.get(subject=sub, session=ses, datatype='fmap', extension='json')]
        fmap_dirs=list(set(fmap_path))
        
        for fmap_dir in fmap_dirs:
            # Construct corresponding destination path in targ_bids_folder
            relative_path = os.path.relpath(fmap_dir, src_bids_folder)
            dest_dir = os.path.join(targ_bids_folder, relative_path)
            
            # Ensure destination directory exists
            os.makedirs(dest_dir, exist_ok=True)
            
            # Run rsync command to copy fmap folder
            rsync_command = ["rsync", "-av", "--exclude=*_orig.json", fmap_dir + "/", dest_dir + "/"]
            subprocess.run(rsync_command, check=True)
            print(f"Copied {fmap_dir} to {dest_dir}")
    else:
        print(f"Copy fmap folders Working on whole {targ_bids_dir}")
        
        # Get all fmap directories by finding fmap files and extracting unique directories
        fmap_path=[i.dirname for i in layout.get(subject=sub, session=ses, datatype='fmap', extension='json')]
        fmap_dirs=list(set(fmap_path))
        
        for fmap_dir in fmap_dirs:
            # Construct corresponding destination path in targ_bids_folder
            relative_path = os.path.relpath(fmap_dir, src_bids_folder)
            dest_dir = os.path.join(targ_bids_folder, relative_path)
            
            # Ensure destination directory exists
            os.makedirs(dest_dir, exist_ok=True)
            
            # Run rsync command to copy fmap folder
            rsync_command = ["rsync", "-av", "--exclude=*_orig.json", fmap_dir + "/", dest_dir + "/"]
            subprocess.run(rsync_command, check=True)
            print(f"Copied {fmap_dir} to {dest_dir}")



def copy_scan_tsv(src_bids_folder, targ_bids_folder, sub=None, ses=None):
    # Initialize BIDS Layout
    layout = BIDSLayout(src_bids_folder)
    if (sub or ses):
        print(f"copy_scan_tsv Working on sub-{sub}, ses-{ses}")
        scan_tsv_path=[i for i in layout.get(subject=sub, session=ses, suffix='scans', extension='tsv', return_type='filename')]
        
        for scan_tsv in scan_tsv_path:
            # Construct corresponding destination path in targ_bids_folder
            relative_path = os.path.relpath(scan_tsv, src_bids_folder)
            dest_dir = os.path.join(targ_bids_folder, relative_path)
            print(f'### dest dir is {dest_dir}')
            
            # Ensure destination directory exists
            os.makedirs(os.path.dirname(dest_dir), exist_ok=True)
            # Run rsync command to copy fmap folder
            rsync_command = ["rsync", "-av", scan_tsv, dest_dir]
            subprocess.run(rsync_command, check=True)
            print(f"Copied {scan_tsv} to {os.path.dirname(dest_dir)}")
    else:
        print(f"copy_scan_tsv Working on {targ_bids_folder}")
        # Get all scan.tsv
        scan_tsv_path=[i for i in layout.get(suffix='scans', extension='tsv', return_type='filename')]
        
        for scan_tsv in scan_tsv_path:
            # Construct corresponding destination path in targ_bids_folder
            relative_path = os.path.relpath(scan_tsv, src_bids_folder)
            dest_dir = os.path.join(targ_bids_folder, relative_path)
            print(f'### dest dir is {dest_dir}')
            
            # Ensure destination directory exists
            os.makedirs(os.path.dirname(dest_dir), exist_ok=True)
            # Run rsync command to copy fmap folder
            rsync_command = ["rsync", "-av", scan_tsv, dest_dir]
            subprocess.run(rsync_command, check=True)
            print(f"Copied {scan_tsv} to {os.path.dirname(dest_dir)}")

def main():
    src_bids_folder = '/bcbl/home/public/Gari/VOTCLOC/main_exp/raw_nifti'  # Update this path
    targ_bids_folder = '/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS' # Update this path
    sub='01'
    ses='09'
    copy_fmap_folders(src_bids_folder, targ_bids_folder, sub, ses)
    #sync_bids_component(src_bids_folder, targ_bids_folder)
    copy_scan_tsv(src_bids_folder, targ_bids_folder,sub,ses)
    '''
    Tested Feb 13, it is working, if the scans.tsv is correct. 
    
    needs to add function to make it work for the sub and ses according to the list
    '''            
if __name__ == "__main__":
    main()
