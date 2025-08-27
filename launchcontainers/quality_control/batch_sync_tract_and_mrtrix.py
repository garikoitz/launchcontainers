import os
import subprocess
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

def rsync_single_subject(analysis_dir, sub, ses):
    """Rsync tracts to RTP/mrtrix for one subject"""
    subses_outputdir = Path(analysis_dir) / f'sub-{sub}' / f'ses-{ses}' / 'output'
    
    file_path_tracts = subses_outputdir / "tracts" 
    file_path_rtp = subses_outputdir / "RTP" / "mrtrix"
    
    # Skip if source doesn't exist
    if not file_path_tracts.exists():
        return f"sub-{sub}_ses-{ses}: No tracts directory"
    
    # Create destination directory
    file_path_rtp.mkdir(parents=True, exist_ok=True)
    
    # Run rsync
    cmd = [
        'rsync', '-avzP', '-h',
        str(file_path_tracts) + '/', 
        str(file_path_rtp) + '/'
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        if result.returncode == 0:
            return f"sub-{sub}_ses-{ses}: SUCCESS"
        else:
            return f"sub-{sub}_ses-{ses}: FAILED - {result.stderr}"
    except Exception as e:
        return f"sub-{sub}_ses-{ses}: ERROR - {str(e)}"
    
def find_subseslist(analysis_dir):
    for dirpath, dirnames, filenames in os.walk(analysis_dir):
        for fname in filenames:
            if fname.lower() == 'subseslist.txt':
                return os.path.join(dirpath, fname)
    raise FileNotFoundError(f'No subseslist.txt found under {analysis_dir}')

def batch_rsync_tracts(analysis_dir):
    """Simple batch rsync with 35 workers"""
    
    # Load subseslist
    path_to_subses = find_subseslist(analysis_dir)
    df_subSes = pd.read_csv(path_to_subses, sep=',', dtype=str)
    
    # Prepare tasks
    tasks = []
    for row in df_subSes.itertuples(index=False, name='Pandas'):
        if row.RUN == 'True' and row.dwi == 'True':
            tasks.append((analysis_dir, row.sub, row.ses))
    
    print(f"Processing {len(tasks)} subjects with 35 workers...")
    
    # Run parallel rsync
    results = []
    with ThreadPoolExecutor(max_workers=35) as executor:
        futures = {executor.submit(rsync_single_subject, *task): task for task in tasks}
        
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            print(result)  # Print progress
    
    print(f"\nCompleted! Processed {len(results)} subjects")
    return results

# Usage:
analysis_dir='/bcbl/home/public/DB/devtrajtract/DATA/MINI/nifti/derivatives/rtp2-pipeline_0.2.1_3.0.4rc2/analysis-paper_dv-main'
results = batch_rsync_tracts(analysis_dir)

