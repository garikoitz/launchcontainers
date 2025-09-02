import shutil
import zipfile
from pathlib import Path
import logging
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from pathlib import Path
logger = logging.getLogger(__name__)


def find_subseslist(analysis_dir):
    for dirpath, dirnames, filenames in os.walk(analysis_dir):
        for fname in filenames:
            if fname.lower() == 'subseslist.txt':
                return os.path.join(dirpath, fname)
    raise FileNotFoundError(f'No subseslist.txt found under {analysis_dir}')

def flatten_directory(directory):
    """Ensure only one level of directory structure"""
    directory = Path(directory)
    items = list(directory.iterdir())
    if len(items) == 1 and items[0].is_dir():
        subdir = items[0]
        for item in subdir.iterdir():
            shutil.move(str(item), str(directory / item.name))
        subdir.rmdir()

def check_and_unzip_tract(subses_outputdir):
    """
    Check for tract directory or zip file and unzip if needed
    
    Args:
        subses_outputdir (Path): Path to the output directory
        
    Returns:
        tuple: (has_tract_dir, has_tract_zip, unzip_success, warning_msg)
    """
    subses_outputdir = Path(subses_outputdir)
    tract_dir = subses_outputdir / "RTP_PIPELINE_ALL_OUTPUT"
    tract_zip = subses_outputdir / "RTP_PIPELINE_ALL_OUTPUT.zip"

    has_tract_dir = tract_dir.exists() and tract_dir.is_dir()
    has_tract_zip = tract_zip.exists() and tract_zip.is_file()
    unzip_success = False
    warning_msg = ""
    
    if has_tract_dir and has_tract_zip:
        logger.info("Both tract/ and tract.zip exist, skipping")
        return has_tract_dir, has_tract_zip, True, ""
    
    elif not has_tract_dir and has_tract_zip:
        try:
            logger.info("Unzipping tract.zip")
            with zipfile.ZipFile(tract_zip, 'r') as zip_ref:
                tract_dir.mkdir(parents=True, exist_ok=True)
                zip_ref.extractall(tract_dir)
                flatten_directory(tract_dir)
                
            unzip_success = True
            has_tract_dir = True
            logger.info("Successfully unzipped tract.zip")
            
        except Exception as e:
            warning_msg = f"Failed to unzip tract.zip: {e}"
            logger.error(warning_msg)
            
    else:
        warning_msg = "Missing files (no tract/ or tract.zip)"
        logger.warning(warning_msg)
        
    return has_tract_dir, has_tract_zip, unzip_success, warning_msg

"""
Parallel processing the unzipping
"""

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_single_subject(analysis_dir, sub, ses, run, dwi):
    """Process a single subject-session for tract unzipping"""
    try:
        if run == 'True' and dwi == 'True':
            subses_outputdir = os.path.join(
                analysis_dir,
                f'sub-{sub}',
                f'ses-{ses}',
                'output'
            )
            
            # Call your check_and_unzip_tract function
            result = check_and_unzip_tract(subses_outputdir)
            
            logger.info(f"Processed sub-{sub}_ses-{ses}: Success")
            return {
                'subject': sub,
                'session': ses,
                'success': True,
                'result': result,
                'output_dir': subses_outputdir
            }
        else:
            logger.info(f"Skipped sub-{sub}_ses-{ses}: RUN={run}, dwi={dwi}")
            return {
                'subject': sub,
                'session': ses,
                'success': True,
                'skipped': True,
                'reason': f"RUN={run}, dwi={dwi}"
            }
            
    except Exception as e:
        logger.error(f"Error processing sub-{sub}_ses-{ses}: {str(e)}")
        return {
            'subject': sub,
            'session': ses,
            'success': False,
            'error': str(e)
        }

def unzipping_tracts_parallel(analysis_dir, n_workers=35):
    """
    Unzip tracts in parallel using concurrent.futures
    
    Args:
        analysis_dir (str): Path to analysis directory
        n_workers (int): Number of parallel workers (default: 35)
    """
    
    logger.info(f"Starting parallel tract unzipping with {n_workers} workers")
    
    # Load subject-session list
    path_to_subses = find_subseslist(analysis_dir)
    df_subSes = pd.read_csv(path_to_subses, sep=',', dtype=str)
    
    logger.info(f"Found {len(df_subSes)} subject-session entries")
    
    # Filter for entries that need processing
    valid_entries = df_subSes[(df_subSes['RUN'] == 'True') & (df_subSes['dwi'] == 'True')]
    logger.info(f"Processing {len(valid_entries)} valid entries")
    
    # Prepare tasks
    tasks = []
    for row in df_subSes.itertuples(index=False, name='Pandas'):
        tasks.append((analysis_dir, row.sub, row.ses, row.RUN, row.dwi))
    
    # Execute in parallel
    results = []
    failed_subjects = []
    skipped_subjects = []
    
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        # Submit all tasks
        future_to_task = {
            executor.submit(process_single_subject, *task): task 
            for task in tasks
        }
        
        # Process completed tasks
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            sub, ses = task[1], task[2]  # Extract subject and session
            
            try:
                result = future.result()
                results.append(result)
                
                if not result['success']:
                    failed_subjects.append(f"sub-{sub}_ses-{ses}")
                elif result.get('skipped', False):
                    skipped_subjects.append(f"sub-{sub}_ses-{ses}")
                    
            except Exception as e:
                logger.error(f"Future failed for sub-{sub}_ses-{ses}: {str(e)}")
                failed_subjects.append(f"sub-{sub}_ses-{ses}")
                results.append({
                    'subject': sub,
                    'session': ses,
                    'success': False,
                    'error': str(e)
                })
    
    # Summary statistics
    total_processed = len(results)
    successful = len([r for r in results if r['success'] and not r.get('skipped', False)])
    failed = len(failed_subjects)
    skipped = len(skipped_subjects)
    
    logger.info(f"Processing complete:")
    logger.info(f"  Total entries: {total_processed}")
    logger.info(f"  Successfully processed: {successful}")
    logger.info(f"  Failed: {failed}")
    logger.info(f"  Skipped: {skipped}")
    
    if failed_subjects:
        logger.warning(f"Failed subjects: {', '.join(failed_subjects)}")
    
    # Save results summary
    save_processing_summary(analysis_dir, results)
    
    return results

def save_processing_summary(analysis_dir, results):
    """Save processing summary to file"""
    import json
    from datetime import datetime
    
    summary_file = Path(analysis_dir) / f"unzipping_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    summary = {
        'timestamp': datetime.now().isoformat(),
        'total_processed': len(results),
        'successful': len([r for r in results if r['success'] and not r.get('skipped', False)]),
        'failed': len([r for r in results if not r['success']]),
        'skipped': len([r for r in results if r.get('skipped', False)]),
        'detailed_results': results
    }
    
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info(f"Summary saved to: {summary_file}")



def check_and_unzip_tract_wrapper(output_dir, sub, ses):
    """Wrapper function for ProcessPoolExecutor"""
    try:
        result = check_and_unzip_tract(output_dir)
        return {
            'subject': sub,
            'session': ses,
            'success': True,
            'result': result,
            'output_dir': output_dir
        }
    except Exception as e:
        return {
            'subject': sub,
            'session': ses,
            'success': False,
            'error': str(e)
        }


# Usage example:
if __name__ == "__main__":
    analysis_dir = "/path/to/your/analysis"
    
    # Use ThreadPoolExecutor (recommended for I/O-bound operations like unzipping)
    results = unzipping_tracts_parallel(analysis_dir, n_workers=35)






"""
# Optimized version with progress tracking
def unzipping_tracts_with_progress(analysis_dir, n_workers=35):

    #Version with progress tracking using tqdm (optional)

    try:
        from tqdm import tqdm
        use_tqdm = True
    except ImportError:
        use_tqdm = False
        logger.warning("tqdm not available, no progress bar will be shown")
    
    logger.info(f"Starting parallel tract unzipping with {n_workers} workers")
    
    path_to_subses = find_subseslist(analysis_dir)
    df_subSes = pd.read_csv(path_to_subses, sep=',', dtype=str)
    
    # Filter valid entries
    valid_entries = df_subSes[(df_subSes['RUN'] == 'True') & (df_subSes['dwi'] == 'True')]
    logger.info(f"Processing {len(valid_entries)} valid entries")
    
    tasks = []
    for row in valid_entries.itertuples(index=False, name='Pandas'):
        tasks.append((analysis_dir, row.sub, row.ses, row.RUN, row.dwi))
    
    results = []
    
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        # Submit all tasks
        future_to_task = {
            executor.submit(process_single_subject, *task): task 
            for task in tasks
        }
        
        # Progress bar setup
        if use_tqdm:
            progress_bar = tqdm(total=len(tasks), desc="Unzipping tracts")
        
        # Process completed tasks
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            try:
                result = future.result()
                results.append(result)
                
                if use_tqdm:
                    progress_bar.update(1)
                    progress_bar.set_postfix({
                        'Current': f"sub-{result['subject']}_ses-{result['session']}",
                        'Success': result['success']
                    })
                    
            except Exception as e:
                logger.error(f"Task failed: {str(e)}")
                if use_tqdm:
                    progress_bar.update(1)
        
        if use_tqdm:
            progress_bar.close()
    
    # Save results
    save_processing_summary(analysis_dir, results)
    return results

    # Alternative version using ProcessPoolExecutor (for CPU-intensive tasks)
def unzipping_tracts_parallel_processes(analysis_dir, n_workers=35):
 
    # Version using ProcessPoolExecutor instead of ThreadPoolExecutor
    # Use this if check_and_unzip_tract is CPU-intensive rather than I/O bound

    from concurrent.futures import ProcessPoolExecutor
    
    logger.info(f"Starting parallel tract unzipping with {n_workers} processes")
    
    path_to_subses = find_subseslist(analysis_dir)
    df_subSes = pd.read_csv(path_to_subses, sep=',', dtype=str)
    
    # Prepare tasks
    tasks = []
    for row in df_subSes.itertuples(index=False, name='Pandas'):
        if row.RUN == 'True' and row.dwi == 'True':
            subses_outputdir = os.path.join(
                analysis_dir,
                f'sub-{row.sub}',
                f'ses-{row.ses}',
                'output'
            )
            tasks.append((subses_outputdir, row.sub, row.ses))
    
    logger.info(f"Processing {len(tasks)} valid entries")
    
    results = []
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        # Submit all tasks
        future_to_task = {
            executor.submit(check_and_unzip_tract_wrapper, task[0], task[1], task[2]): task 
            for task in tasks
        }
        
        # Process completed tasks
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            try:
                result = future.result()
                results.append(result)
                logger.info(f"Completed: sub-{task[1]}_ses-{task[2]}")
            except Exception as e:
                logger.error(f"Failed: sub-{task[1]}_ses-{task[2]} - {str(e)}")
                results.append({
                    'subject': task[1],
                    'session': task[2],
                    'success': False,
                    'error': str(e)
                })
    
    return results

"""