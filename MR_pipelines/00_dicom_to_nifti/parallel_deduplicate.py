#!/usr/bin/env python3
"""
Parallel DICOM session deduplication with intelligent date-based and pattern-based grouping.
Handles cases where duplicates have different acquisition dates.
"""

import sys
import hashlib
import re
import shutil
from pathlib import Path
from collections import defaultdict, Counter
import pydicom
from typing import Dict, List, Tuple, Optional, Set
import argparse
from multiprocessing import Pool, cpu_count
import time
from datetime import datetime

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


def extract_filename_pattern(filename: str) -> str:
    """Extract naming pattern from filename with detailed UID sub-patterns."""
    name = Path(filename).stem
    
    # Check for UID-based patterns (1.3.12.2.1107...)
    if re.match(r'^1\.3\.12\.2\.1107\.', name):
        uid_match = re.match(r'^1\.3\.12\.2\.1107\.5\.2\.43\.167004\.(\d+)', name)
        if uid_match:
            timestamp_part = uid_match.group(1)
            
            if timestamp_part.startswith('2025'):
                return 'uid_timestamp_short'
            elif timestamp_part.startswith('3000'):
                return 'uid_timestamp_long'
            else:
                return 'uid_pattern'
        else:
            return 'uid_pattern'
    
    # Standard naming patterns
    elif re.match(r'^IM[_-]?\d+', name, re.IGNORECASE):
        return 'im_pattern'
    elif re.match(r'^IMG[_-]?\d+', name, re.IGNORECASE):
        return 'img_pattern'
    elif re.match(r'^slice[_-]?\d+', name, re.IGNORECASE):
        return 'slice_pattern'
    elif re.match(r'^MR[_-]?\d+', name, re.IGNORECASE):
        return 'mr_pattern'
    elif re.match(r'^\d{4,}\.dcm?$', name + '.dcm'):
        return 'numeric_pattern'
    elif re.match(r'^[a-zA-Z]+\d+', name):
        return 'prefix_numeric_pattern'
    else:
        return 'other_pattern'


def get_pattern_description(pattern: str) -> str:
    """Get human-readable description of pattern."""
    descriptions = {
        'uid_timestamp_short': 'UID with short timestamp (2025...) [DEFAULT]',
        'uid_timestamp_long': 'UID with long timestamp (3000...)',
        'uid_pattern': 'UID-based filenames (generic)',
        'im_pattern': 'IM-prefixed filenames',
        'img_pattern': 'IMG-prefixed filenames',
        'slice_pattern': 'Slice-prefixed filenames',
        'mr_pattern': 'MR-prefixed filenames',
        'numeric_pattern': 'Pure numeric filenames',
        'prefix_numeric_pattern': 'Prefix+number filenames',
        'other_pattern': 'Other naming patterns',
    }
    return descriptions.get(pattern, pattern)


def get_acquisition_datetime(dcm_path: Path) -> Optional[datetime]:
    """
    Extract acquisition date and time from DICOM file.
    
    Returns:
        datetime object or None if not available
    """
    try:
        ds = pydicom.dcmread(str(dcm_path), force=True)
        
        # Try different date/time fields in order of preference
        # 1. Content Date/Time (most specific)
        date_str = getattr(ds, 'ContentDate', None)
        time_str = getattr(ds, 'ContentTime', None)
        
        # 2. Series Date/Time
        if not date_str:
            date_str = getattr(ds, 'SeriesDate', None)
        if not time_str:
            time_str = getattr(ds, 'SeriesTime', None)
        
        # 3. Study Date/Time
        if not date_str:
            date_str = getattr(ds, 'StudyDate', None)
        if not time_str:
            time_str = getattr(ds, 'StudyTime', None)
        
        if date_str and time_str:
            # Parse DICOM date (YYYYMMDD) and time (HHMMSS.ffffff)
            date_str = str(date_str)
            time_str = str(time_str).split('.')[0]  # Remove microseconds for simplicity
            
            if len(date_str) == 8 and len(time_str) >= 6:
                dt_str = f"{date_str}{time_str[:6]}"
                return datetime.strptime(dt_str, '%Y%m%d%H%M%S')
        
        return None
        
    except Exception as e:
        return None


def find_all_dcm_folders(session_folders: List[Path]) -> List[Tuple[Path, Path, int]]:
    """Find all DICOM folders across multiple sessions with file counts."""
    all_dcm_folders = []
    
    for session_folder in session_folders:
        if not session_folder.exists() or not session_folder.is_dir():
            print(f"Warning: Skipping invalid path: {session_folder}", file=sys.stderr)
            continue
        
        for item in session_folder.iterdir():
            if item.is_dir():
                dcm_files = list(item.glob("*.dcm")) + \
                           list(item.glob("*.DCM")) + \
                           list(item.glob("*.IMA"))
                
                if dcm_files:
                    all_dcm_folders.append((session_folder, item, len(dcm_files)))
    
    return all_dcm_folders


def analyze_patterns_with_default(dcm_folders_info: List[Tuple[Path, Path, int]], 
                                  sample_size: int = 300) -> Tuple[str, Dict[str, int]]:
    """Pattern analysis with default preference for shorter UID pattern."""
    all_patterns = Counter()
    sampled = 0
    
    for _, dcm_folder, _ in dcm_folders_info:
        dcm_files = list(dcm_folder.glob("*.dcm")) + \
                   list(dcm_folder.glob("*.DCM")) + \
                   list(dcm_folder.glob("*.IMA"))
        
        for dcm_file in dcm_files[:10]:
            pattern = extract_filename_pattern(dcm_file.name)
            all_patterns[pattern] += 1
            sampled += 1
            
            if sampled >= sample_size:
                break
        
        if sampled >= sample_size:
            break
    
    # DEFAULT: If uid_timestamp_short exists, use it
    if 'uid_timestamp_short' in all_patterns:
        return 'uid_timestamp_short', dict(all_patterns)
    
    # Fallback priority order
    priority_order = [
        'im_pattern', 'img_pattern', 'mr_pattern', 'slice_pattern',
        'numeric_pattern', 'prefix_numeric_pattern',
        'uid_timestamp_long', 'uid_pattern', 'other_pattern',
    ]
    
    if all_patterns:
        selected_pattern = max(all_patterns.keys(), 
                              key=lambda p: (all_patterns[p], 
                                           -priority_order.index(p) if p in priority_order else 999))
    else:
        selected_pattern = 'uid_pattern'
    
    return selected_pattern, dict(all_patterns)


def get_pixel_hash_with_metadata(dcm_path: Path, hash_algo: str = 'md5') -> Tuple[Optional[str], Optional[Dict]]:
    """
    Calculate hash of pixel data and extract comprehensive metadata.
    
    Returns:
        Tuple of (pixel_hash, metadata_dict) or (None, None) on error
    """
    try:
        ds = pydicom.dcmread(str(dcm_path), force=True)
        arr = ds.pixel_array
        
        # Calculate hash
        if hash_algo == 'md5':
            pixel_hash = hashlib.md5(arr.tobytes()).hexdigest()
        else:
            pixel_hash = hashlib.sha256(arr.tobytes()).hexdigest()
        
        # Extract metadata including acquisition datetime
        metadata = {
            'instance_number': getattr(ds, 'InstanceNumber', 'N/A'),
            'file_size': dcm_path.stat().st_size,
            'pattern': extract_filename_pattern(dcm_path.name),
        }
        
        # Get acquisition datetime
        acq_datetime = get_acquisition_datetime(dcm_path)
        if acq_datetime:
            metadata['acq_datetime'] = acq_datetime
        
        return pixel_hash, metadata
        
    except Exception as e:
        return None, None


def select_file_by_date_and_pattern(files: List[Tuple[Path, Dict]], 
                                    preferred_pattern: str) -> Path:
    """
    Select which file to keep based on date and pattern.
    
    Strategy:
    1. Check if all files have the same acquisition date
    2. If same date: prefer shorter pattern name (uid_timestamp_short)
    3. If different dates: keep the newest file
    
    Args:
        files: List of (file_path, metadata) tuples with same pixel data
        preferred_pattern: Pattern to prefer when dates are same
    
    Returns:
        Path to selected file
    """
    # Extract dates from metadata
    files_with_dates = [(f, m) for f, m in files if 'acq_datetime' in m]
    files_without_dates = [(f, m) for f, m in files if 'acq_datetime' not in m]
    
    # If no files have dates, fall back to pattern-based selection
    if not files_with_dates:
        return select_file_by_pattern_only(files, preferred_pattern)
    
    # Check if all dates are the same
    dates = [m['acq_datetime'] for _, m in files_with_dates]
    unique_dates = set(dates)
    
    if len(unique_dates) == 1:
        # All files have the same date - prefer shorter pattern
        # Priority: shorter patterns first
        pattern_length_priority = {
            'uid_timestamp_short': 1,
            'im_pattern': 2,
            'img_pattern': 3,
            'mr_pattern': 4,
            'slice_pattern': 5,
            'numeric_pattern': 6,
            'uid_timestamp_long': 7,
            'prefix_numeric_pattern': 8,
            'uid_pattern': 9,
            'other_pattern': 10,
        }
        
        # Sort by pattern priority (shorter/cleaner first)
        files_sorted = sorted(files_with_dates, 
                            key=lambda x: pattern_length_priority.get(x[1]['pattern'], 99))
        return files_sorted[0][0]
    
    else:
        # Different dates - keep the newest file
        newest_file = max(files_with_dates, key=lambda x: x[1]['acq_datetime'])
        return newest_file[0]


def select_file_by_pattern_only(files: List[Tuple[Path, Dict]], 
                                preferred_pattern: str) -> Path:
    """
    Select which file to keep based only on naming pattern.
    Fallback when no date information is available.
    """
    # First pass: exact pattern match
    for file_path, metadata in files:
        if metadata.get('pattern') == preferred_pattern:
            return file_path
    
    # Second pass: if preferred is UID sub-pattern, accept any UID pattern
    if preferred_pattern.startswith('uid_'):
        for file_path, metadata in files:
            pattern = metadata.get('pattern', '')
            if pattern.startswith('uid_'):
                return file_path
    
    # Fallback: return first file
    return files[0][0]


def process_single_dcm_folder(args: Tuple) -> Dict:
    """
    Process a single DICOM folder with date-aware duplicate detection.
    
    Args:
        args: Tuple of (session_folder, dcm_folder, output_base, 
                       preferred_pattern, hash_algo)
    
    Returns:
        Dictionary with processing results
    """
    session_folder, dcm_folder, output_base, preferred_pattern, hash_algo = args
    
    try:
        relative_path = dcm_folder.relative_to(session_folder)
        
        # Find all DICOM files
        dcm_files = list(dcm_folder.glob("*.dcm")) + \
                   list(dcm_folder.glob("*.DCM")) + \
                   list(dcm_folder.glob("*.IMA"))
        
        if not dcm_files:
            return {
                'session': session_folder.name,
                'folder': dcm_folder.name,
                'status': 'empty',
                'total_files': 0,
                'unique_files': 0,
                'duplicates': 0,
                'same_date_duplicates': 0,
                'diff_date_duplicates': 0,
            }
        
        # Step 1: Group files by naming pattern
        pattern_groups = defaultdict(list)
        for dcm_file in dcm_files:
            pattern = extract_filename_pattern(dcm_file.name)
            pattern_groups[pattern].append(dcm_file)
        
        # Step 2: Calculate pixel hashes with metadata (including dates)
        hash_to_files = defaultdict(list)
        
        for dcm_file in dcm_files:
            pixel_hash, metadata = get_pixel_hash_with_metadata(dcm_file, hash_algo)
            if pixel_hash and metadata:
                hash_to_files[pixel_hash].append((dcm_file, metadata))
        
        # Step 3: Select unique files based on date and pattern
        files_to_keep = set()
        duplicates_count = 0
        same_date_dups = 0
        diff_date_dups = 0
        
        for pixel_hash, files in hash_to_files.items():
            if len(files) > 1:
                # Check if files have different dates
                files_with_dates = [(f, m) for f, m in files if 'acq_datetime' in m]
                
                if files_with_dates:
                    dates = [m['acq_datetime'] for _, m in files_with_dates]
                    unique_dates = set(dates)
                    
                    if len(unique_dates) == 1:
                        same_date_dups += len(files) - 1
                    else:
                        diff_date_dups += len(files) - 1
                else:
                    same_date_dups += len(files) - 1
                
                # Select file based on date and pattern
                selected_file = select_file_by_date_and_pattern(files, preferred_pattern)
                files_to_keep.add(selected_file)
                duplicates_count += len(files) - 1
            else:
                # No duplicates
                files_to_keep.add(files[0][0])
        
        # Step 4: Create output directory and copy files
        output_session = output_base / session_folder.name
        output_dcm_folder = output_session / relative_path
        output_dcm_folder.mkdir(parents=True, exist_ok=True)
        
        for file_to_keep in files_to_keep:
            dest_file = output_dcm_folder / file_to_keep.name
            shutil.copy2(file_to_keep, dest_file)
        
        return {
            'session': session_folder.name,
            'folder': dcm_folder.name,
            'status': 'success',
            'total_files': len(dcm_files),
            'unique_files': len(files_to_keep),
            'duplicates': duplicates_count,
            'same_date_duplicates': same_date_dups,
            'diff_date_duplicates': diff_date_dups,
            'pattern_groups': len(pattern_groups),
        }
        
    except Exception as e:
        return {
            'session': session_folder.name,
            'folder': dcm_folder.name,
            'status': 'error',
            'error': str(e),
            'total_files': 0,
            'unique_files': 0,
            'duplicates': 0,
            'same_date_duplicates': 0,
            'diff_date_duplicates': 0,
        }


def sort_by_workload(dcm_folders_info: List[Tuple[Path, Path, int]]) -> List[Tuple[Path, Path, int]]:
    """Sort DICOM folders by file count (descending) for load balancing."""
    return sorted(dcm_folders_info, key=lambda x: x[2], reverse=True)


def process_sessions_parallel(session_folders: List[Path],
                              output_base: Path,
                              preferred_pattern: Optional[str] = None,
                              hash_algo: str = 'md5',
                              n_jobs: int = 40) -> Dict:
    """Process multiple sessions in parallel with date-aware deduplication."""
    
    print("=" * 80)
    print("PARALLEL DICOM SESSION DEDUPLICATION (DATE-AWARE)")
    print("=" * 80)
    print(f"Sessions to process: {len(session_folders)}")
    print(f"Output base: {output_base}")
    print(f"Parallel workers: {n_jobs}")
    print(f"Hash algorithm: {hash_algo.upper()}")
    print(f"Deduplication strategy:")
    print(f"  - Same date: prefer shorter pattern name")
    print(f"  - Different dates: keep newest acquisition")
    print("=" * 80)
    print()
    
    # Step 1: Discover all DICOM folders
    print("Step 1: Discovering DICOM folders...")
    start_time = time.time()
    
    dcm_folders_info = find_all_dcm_folders(session_folders)
    
    if not dcm_folders_info:
        print("No DICOM folders found!")
        return {}
    
    total_files = sum(count for _, _, count in dcm_folders_info)
    
    print(f"  Found {len(dcm_folders_info)} DICOM folders")
    print(f"  Total DICOM files: {total_files:,}")
    print(f"  Discovery time: {time.time() - start_time:.2f}s")
    print()
    
    # Step 2: Analyze naming patterns
    print("Step 2: Analyzing naming patterns...")
    start_time = time.time()
    
    if preferred_pattern:
        selected_pattern = preferred_pattern
        print(f"  Using user-specified pattern: {get_pattern_description(selected_pattern)}")
    else:
        selected_pattern, pattern_counts = analyze_patterns_with_default(dcm_folders_info)
        print(f"  Pattern distribution detected:")
        for pattern, count in sorted(pattern_counts.items(), key=lambda x: -x[1]):
            marker = " ← SELECTED (DEFAULT)" if pattern == selected_pattern and pattern == 'uid_timestamp_short' else \
                    " ← SELECTED" if pattern == selected_pattern else ""
            print(f"    {get_pattern_description(pattern)}: {count} files{marker}")
        print()
        print(f"  Selected pattern: {get_pattern_description(selected_pattern)}")
        
        if selected_pattern == 'uid_timestamp_short':
            print(f"  → Using shorter UID pattern for consistent naming")
    
    print(f"  Analysis time: {time.time() - start_time:.2f}s")
    print()
    
    # Step 3: Optimize work distribution
    print("Step 3: Optimizing work distribution...")
    dcm_folders_info = sort_by_workload(dcm_folders_info)
    
    size_categories = {
        'large (>200 files)': sum(1 for _, _, c in dcm_folders_info if c > 200),
        'medium (10-200 files)': sum(1 for _, _, c in dcm_folders_info if 10 <= c <= 200),
        'small (<10 files)': sum(1 for _, _, c in dcm_folders_info if c < 10),
    }
    
    print("  Workload distribution:")
    for category, count in size_categories.items():
        print(f"    {category}: {count} folders")
    print()
    
    # Step 4: Create output directory
    output_base.mkdir(parents=True, exist_ok=True)
    
    # Step 5: Parallel processing
    print("Step 4: Processing folders in parallel (with date checking)...")
    print(f"  Strategy: Largest folders first")
    print(f"  Workers: {n_jobs}")
    print(f"  Pattern preference: {get_pattern_description(selected_pattern)}")
    print()
    
    start_time = time.time()
    
    # Prepare work items
    work_items = [
        (session_folder, dcm_folder, output_base, selected_pattern, hash_algo)
        for session_folder, dcm_folder, _ in dcm_folders_info
    ]
    
    # Process in parallel
    results = []
    
    with Pool(processes=n_jobs) as pool:
        if HAS_TQDM:
            results = list(tqdm(
                pool.imap_unordered(process_single_dcm_folder, work_items),
                total=len(work_items),
                desc="Processing folders",
                unit="folder"
            ))
        else:
            for i, result in enumerate(pool.imap_unordered(process_single_dcm_folder, work_items), 1):
                results.append(result)
                if i % 50 == 0 or i == len(work_items):
                    print(f"  Processed {i}/{len(work_items)} folders...")
    
    processing_time = time.time() - start_time
    
    # Compile statistics
    stats = {
        'total_folders': len(dcm_folders_info),
        'total_files': 0,
        'unique_files': 0,
        'duplicates_removed': 0,
        'same_date_duplicates': 0,
        'diff_date_duplicates': 0,
        'folders_with_duplicates': 0,
        'errors': 0,
        'processing_time': processing_time,
        'selected_pattern': selected_pattern,
        'sessions': defaultdict(lambda: {
            'folders': 0,
            'total_files': 0,
            'unique_files': 0,
            'duplicates': 0,
            'same_date_dups': 0,
            'diff_date_dups': 0,
        })
    }
    
    for result in results:
        if result['status'] == 'error':
            stats['errors'] += 1
            print(f"  Error in {result['session']}/{result['folder']}: {result.get('error', 'Unknown')}", 
                  file=sys.stderr)
        else:
            session_name = result['session']
            stats['total_files'] += result['total_files']
            stats['unique_files'] += result['unique_files']
            stats['duplicates_removed'] += result['duplicates']
            stats['same_date_duplicates'] += result.get('same_date_duplicates', 0)
            stats['diff_date_duplicates'] += result.get('diff_date_duplicates', 0)
            
            if result['duplicates'] > 0:
                stats['folders_with_duplicates'] += 1
            
            stats['sessions'][session_name]['folders'] += 1
            stats['sessions'][session_name]['total_files'] += result['total_files']
            stats['sessions'][session_name]['unique_files'] += result['unique_files']
            stats['sessions'][session_name]['duplicates'] += result['duplicates']
            stats['sessions'][session_name]['same_date_dups'] += result.get('same_date_duplicates', 0)
            stats['sessions'][session_name]['diff_date_dups'] += result.get('diff_date_duplicates', 0)
    
    return stats


def display_final_summary(stats: Dict):
    """Display comprehensive processing summary."""
    print()
    print("=" * 80)
    print("PROCESSING COMPLETE")
    print("=" * 80)
    print()
    
    print("Overall Statistics:")
    print("-" * 80)
    print(f"  Pattern used: {get_pattern_description(stats['selected_pattern'])}")
    print(f"  Total folders processed: {stats['total_folders']}")
    print(f"  Folders with duplicates: {stats['folders_with_duplicates']}")
    print(f"  Total DICOM files: {stats['total_files']:,}")
    print(f"  Unique files kept: {stats['unique_files']:,}")
    print(f"  Duplicate files removed: {stats['duplicates_removed']:,}")
    print()
    print(f"  Duplicate breakdown:")
    print(f"    Same date (pattern preference): {stats['same_date_duplicates']:,}")
    print(f"    Different dates (kept newest): {stats['diff_date_duplicates']:,}")
    
    if stats['total_files'] > 0:
        dup_rate = (stats['duplicates_removed'] / stats['total_files']) * 100
        print(f"  Overall duplication rate: {dup_rate:.1f}%")
    
    if stats['errors'] > 0:
        print(f"  Errors encountered: {stats['errors']}")
    
    print()
    print(f"Processing time: {stats['processing_time']:.2f}s ({stats['processing_time']/60:.2f} min)")
    
    if stats['total_files'] > 0:
        throughput = stats['total_files'] / stats['processing_time']
        print(f"Throughput: {throughput:.1f} files/second")
    
    print()
    print("Per-Session Statistics:")
    print("-" * 80)
    
    for session_name, session_stats in sorted(stats['sessions'].items()):
        dup_rate = 0
        if session_stats['total_files'] > 0:
            dup_rate = (session_stats['duplicates'] / session_stats['total_files']) * 100
        
        print(f"  {session_name}:")
        print(f"    Folders: {session_stats['folders']}")
        print(f"    Files: {session_stats['total_files']:,} → {session_stats['unique_files']:,} "
              f"(removed {session_stats['duplicates']:,}, {dup_rate:.1f}%)")
        if session_stats['same_date_dups'] > 0 or session_stats['diff_date_dups'] > 0:
            print(f"    Duplicates: same-date={session_stats['same_date_dups']:,}, "
                  f"diff-date={session_stats['diff_date_dups']:,}")
    
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description='Parallel DICOM deduplication with date-aware selection',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Deduplication Strategy:
  1. Group files by naming pattern
  2. Check acquisition dates for duplicates
  3. If same date: prefer shorter pattern (uid_timestamp_short by default)
  4. If different dates: keep the NEWEST acquisition
  
This handles cases where the same scan was uploaded multiple times
with different timestamps after a system update.

Examples:
  # Use default (date-aware, shorter UID preferred)
  python parallel_deduplicate.py /path/to/ses-* -o /path/to/output -j 40
  
  # Force specific pattern preference
  python parallel_deduplicate.py /path/to/ses-* -o /path/to/output -j 40 \\
      --prefer-pattern uid_timestamp_long
        """
    )
    
    parser.add_argument('session_folders', nargs='+', type=str,
                       help='Path(s) to session folder(s)')
    parser.add_argument('-o', '--output', type=str, required=True,
                       help='Output base directory')
    parser.add_argument('-j', '--jobs', type=int, default=None,
                       help=f'Number of parallel workers (default: {cpu_count()})')
    parser.add_argument('--prefer-pattern', '-p', type=str,
                       choices=['uid_timestamp_short', 'uid_timestamp_long', 'uid_pattern',
                               'im_pattern', 'img_pattern', 'mr_pattern', 
                               'slice_pattern', 'numeric_pattern', 'prefix_numeric_pattern', 
                               'other_pattern'],
                       help='Override default pattern preference')
    parser.add_argument('--hash', choices=['sha256', 'md5'], default='md5',
                       help='Hash algorithm (default: md5 for speed)')
    
    args = parser.parse_args()
    
    # Validate inputs
    session_folders = [Path(p).resolve() for p in args.session_folders]
    output_base = Path(args.output).resolve()
    
    invalid_folders = [f for f in session_folders if not f.exists() or not f.is_dir()]
    if invalid_folders:
        print("Error: Invalid session paths:")
        for f in invalid_folders:
            print(f"  {f}")
        sys.exit(1)
    
    n_jobs = args.jobs if args.jobs else cpu_count()
    
    if output_base.exists():
        print(f"Warning: Output directory exists: {output_base}")
        response = input("Continue and potentially overwrite? (yes/no): ")
        if response.lower() != 'yes':
            print("Aborted.")
            sys.exit(0)
        print()
    
    # Process sessions
    stats = process_sessions_parallel(
        session_folders,
        output_base,
        preferred_pattern=args.prefer_pattern,
        hash_algo=args.hash,
        n_jobs=n_jobs
    )
    
    # Display results
    if stats:
        display_final_summary(stats)
        print()
        print(f"✓ Deduplicated sessions saved to: {output_base}")
        print(f"✓ Strategy: same-date → shorter pattern, diff-date → newest")
    
    sys.exit(0)


if __name__ == "__main__":
    main()
