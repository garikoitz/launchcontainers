#!/usr/bin/env python3
"""
Batch DICOM deduplication across multiple sessions using a session list file.
Calls parallel_deduplicate.py for each session or group of sessions.
"""

import sys
import subprocess
from pathlib import Path
from typing import List, Tuple
import argparse
import time


def read_session_list(seslist_file: Path) -> List[str]:
    """
    Read session list from file.
    
    File format (one session per line):
        sub-01/ses-01
        sub-01/ses-02
        sub-02/ses-01
        
    Or with comments:
        # Subject 01
        sub-01/ses-01
        sub-01/ses-02
        
    Returns:
        List of session paths (relative paths)
    """
    sessions = []
    
    with open(seslist_file, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            
            # Basic validation - check for suspicious characters
            if any(c in line for c in ['*', '?', '|', '<', '>']):
                print(f"Warning: Line {line_num} contains suspicious characters: {line}", 
                      file=sys.stderr)
                continue
            
            sessions.append(line)
    
    return sessions


def validate_sessions(base_input: Path, sessions: List[str]) -> Tuple[List[Path], List[str]]:
    """
    Validate that all sessions exist in the input directory.
    
    Returns:
        Tuple of (valid_session_paths, invalid_session_names)
    """
    valid_sessions = []
    invalid_sessions = []
    
    for session in sessions:
        session_path = base_input / session
        
        if session_path.exists() and session_path.is_dir():
            valid_sessions.append(session_path)
        else:
            invalid_sessions.append(session)
    
    return valid_sessions, invalid_sessions


def run_parallel_deduplicate(session_paths: List[Path],
                             output_base: Path,
                             parallel_script: Path,
                             n_jobs: int = 40,
                             hash_algo: str = 'md5',
                             prefer_pattern: str = None) -> int:
    """
    Run the parallel deduplication script.
    
    Returns:
        Return code from the subprocess
    """
    
    # Build command
    cmd = [
        sys.executable,  # Use same Python interpreter as current script
        str(parallel_script),
        *[str(p) for p in session_paths],
        '-o', str(output_base),
        '-j', str(n_jobs),
        '--hash', hash_algo,
    ]
    
    if prefer_pattern:
        cmd.extend(['--prefer-pattern', prefer_pattern])
    
    print(f"Running command:")
    print(f"  {' '.join(cmd[:3])} \\")
    if len(session_paths) <= 3:
        for p in session_paths:
            print(f"    {p} \\")
    else:
        for p in session_paths[:2]:
            print(f"    {p} \\")
        print(f"    ... ({len(session_paths) - 2} more sessions) \\")
    print(f"    -o {output_base} -j {n_jobs} --hash {hash_algo}")
    if prefer_pattern:
        print(f"    --prefer-pattern {prefer_pattern}")
    print()
    
    # Run subprocess
    try:
        result = subprocess.run(cmd, check=False)
        return result.returncode
    except KeyboardInterrupt:
        print("\n\nInterrupted by user (Ctrl+C)")
        return 130  # Standard exit code for SIGINT
    except Exception as e:
        print(f"\nError running parallel_deduplicate.py: {e}", file=sys.stderr)
        return 1


def main():
    parser = argparse.ArgumentParser(
        description='Batch DICOM deduplication using session list (with date-aware logic)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Session List Format:
  Plain text file with one session per line (relative paths):
  
    sub-01/ses-01
    sub-01/ses-02
    sub-01/ses-03
    ...
  
  Comments (lines starting with #) and empty lines are ignored.

Deduplication Strategy:
  The underlying parallel_deduplicate.py uses date-aware logic:
  - Same acquisition date: prefer shorter pattern name
  - Different acquisition dates: keep NEWEST file
  
  This handles Siemens system updates that re-uploaded with new timestamps.

Examples:
  # Basic usage
  python batch_deduplicate_sessions.py \\
      --seslist sessions.txt \\
      --input ~/public/Gari/VOTCLOC/main_exp/dicom \\
      --output ~/public/Gari/VOTCLOC/main_exp/dicom_clean \\
      -j 40
  
  # Dry run to preview
  python batch_deduplicate_sessions.py \\
      --seslist sessions.txt \\
      --input ~/public/Gari/VOTCLOC/main_exp/dicom \\
      --output ~/public/Gari/VOTCLOC/main_exp/dicom_clean \\
      --dry-run
  
  # Use specific parallel script location
  python batch_deduplicate_sessions.py \\
      --seslist sessions.txt \\
      --input ~/public/Gari/VOTCLOC/main_exp/dicom \\
      --output ~/public/Gari/VOTCLOC/main_exp/dicom_clean \\
      --parallel-script /path/to/parallel_deduplicate.py \\
      -j 40
  
  # Force specific pattern preference
  python batch_deduplicate_sessions.py \\
      --seslist sessions.txt \\
      --input ~/public/Gari/VOTCLOC/main_exp/dicom \\
      --output ~/public/Gari/VOTCLOC/main_exp/dicom_clean \\
      --prefer-pattern uid_timestamp_short \\
      -j 40

Session List Example (sessions.txt):
  # VOTCLOC Study - Main Experiment
  # Subject 01 - All sessions
  sub-01/ses-01
  sub-01/ses-02
  sub-01/ses-03
  sub-01/ses-04
  sub-01/ses-05
  sub-01/ses-06
  sub-01/ses-07
  sub-01/ses-08
  
  # Subject 02
  sub-02/ses-01
  sub-02/ses-02
        """
    )
    
    parser.add_argument('--seslist', '-s', type=str, required=True,
                       help='Path to session list file')
    parser.add_argument('--input', '-i', type=str, required=True,
                       help='Base input directory containing sessions')
    parser.add_argument('--output', '-o', type=str, required=True,
                       help='Base output directory for deduplicated sessions')
    parser.add_argument('--parallel-script', type=str, default='parallel_deduplicate.py',
                       help='Path to parallel_deduplicate.py script (default: ./parallel_deduplicate.py)')
    parser.add_argument('-j', '--jobs', type=int, default=40,
                       help='Number of parallel workers (default: 40)')
    parser.add_argument('--hash', choices=['sha256', 'md5'], default='md5',
                       help='Hash algorithm (default: md5 for speed)')
    parser.add_argument('--prefer-pattern', '-p', type=str,
                       choices=['uid_timestamp_short', 'uid_timestamp_long', 'uid_pattern',
                               'im_pattern', 'img_pattern', 'mr_pattern', 
                               'slice_pattern', 'numeric_pattern', 'prefix_numeric_pattern', 
                               'other_pattern'],
                       help='Override default pattern preference')
    parser.add_argument('--dry-run', '-n', action='store_true',
                       help='Show what would be processed without running')
    
    args = parser.parse_args()
    
    # Convert to Path objects
    seslist_file = Path(args.seslist).resolve()
    base_input = Path(args.input).resolve()
    base_output = Path(args.output).resolve()
    parallel_script = Path(args.parallel_script)
    
    # If parallel script is relative, look in current directory
    if not parallel_script.is_absolute():
        parallel_script = Path.cwd() / parallel_script
    
    # Validate inputs
    if not seslist_file.exists():
        print(f"Error: Session list file not found: {seslist_file}")
        sys.exit(1)
    
    if not base_input.exists():
        print(f"Error: Input directory not found: {base_input}")
        sys.exit(1)
    
    if not parallel_script.exists():
        print(f"Error: Parallel script not found: {parallel_script}")
        print(f"Expected location: {parallel_script}")
        print(f"\nMake sure parallel_deduplicate.py is in the same directory or specify --parallel-script")
        sys.exit(1)
    
    # Read session list
    print("=" * 80)
    print("BATCH DICOM DEDUPLICATION (DATE-AWARE)")
    print("=" * 80)
    print(f"Session list: {seslist_file}")
    print(f"Input base: {base_input}")
    print(f"Output base: {base_output}")
    print(f"Parallel script: {parallel_script}")
    print(f"Workers: {args.jobs}")
    print(f"Hash algorithm: {args.hash}")
    if args.prefer_pattern:
        print(f"Pattern preference: {args.prefer_pattern}")
    print()
    print("Deduplication strategy:")
    print("  - Same date: prefer shorter pattern name")
    print("  - Different dates: keep newest acquisition")
    print("=" * 80)
    print()
    
    print("Reading session list...")
    try:
        sessions = read_session_list(seslist_file)
    except Exception as e:
        print(f"Error reading session list: {e}")
        sys.exit(1)
    
    print(f"Found {len(sessions)} sessions in list")
    
    if not sessions:
        print("Error: No valid sessions found in list file!")
        sys.exit(1)
    
    print()
    
    # Validate sessions
    print("Validating sessions...")
    valid_sessions, invalid_sessions = validate_sessions(base_input, sessions)
    
    print(f"Valid sessions: {len(valid_sessions)}")
    if invalid_sessions:
        print(f"Invalid sessions: {len(invalid_sessions)}")
        print("\nThe following sessions were not found:")
        for session in invalid_sessions:
            print(f"  ✗ {session}")
        print()
        
        if not valid_sessions:
            print("Error: No valid sessions found!")
            sys.exit(1)
        
        response = input("Continue with valid sessions only? (yes/no): ")
        if response.lower() != 'yes':
            print("Aborted.")
            sys.exit(1)
    
    print()
    print(f"Sessions to process ({len(valid_sessions)}):")
    # Show first 10 and last 5, or all if <= 15
    if len(valid_sessions) <= 15:
        for session in valid_sessions:
            print(f"  ✓ {session.relative_to(base_input)}")
    else:
        for session in valid_sessions[:10]:
            print(f"  ✓ {session.relative_to(base_input)}")
        print(f"  ... ({len(valid_sessions) - 15} more sessions)")
        for session in valid_sessions[-5:]:
            print(f"  ✓ {session.relative_to(base_input)}")
    print()
    
    # Check output directory
    if base_output.exists():
        print(f"Warning: Output directory already exists: {base_output}")
        response = input("Continue and potentially overwrite? (yes/no): ")
        if response.lower() != 'yes':
            print("Aborted.")
            sys.exit(0)
        print()
    
    if args.dry_run:
        print("=" * 80)
        print("DRY RUN - No files will be processed")
        print("=" * 80)
        print(f"Sessions: {len(valid_sessions)}")
        print(f"Input: {base_input}")
        print(f"Output: {base_output}")
        print(f"Workers: {args.jobs}")
        print()
        print("Command that would be executed:")
        print(f"  python {parallel_script} \\")
        for session in valid_sessions[:3]:
            print(f"    {session} \\")
        if len(valid_sessions) > 3:
            print(f"    ... ({len(valid_sessions) - 3} more) \\")
        print(f"    -o {base_output} -j {args.jobs}")
        print()
        print("Run without --dry-run to execute.")
        sys.exit(0)
    
    # Confirm before proceeding
    print("=" * 80)
    print("READY TO START")
    print("=" * 80)
    print(f"About to process {len(valid_sessions)} sessions with {args.jobs} workers")
    print()
    response = input("Proceed with deduplication? (yes/no): ")
    if response.lower() != 'yes':
        print("Aborted.")
        sys.exit(0)
    print()
    
    # Run deduplication
    print("=" * 80)
    print("STARTING DEDUPLICATION")
    print("=" * 80)
    print()
    
    start_time = time.time()
    
    returncode = run_parallel_deduplicate(
        valid_sessions,
        base_output,
        parallel_script,
        n_jobs=args.jobs,
        hash_algo=args.hash,
        prefer_pattern=args.prefer_pattern
    )
    
    elapsed_time = time.time() - start_time
    
    print()
    print("=" * 80)
    print("BATCH PROCESSING COMPLETE")
    print("=" * 80)
    print(f"Total time: {elapsed_time:.2f}s ({elapsed_time/60:.2f} minutes)")
    print(f"Sessions processed: {len(valid_sessions)}")
    print(f"Output directory: {base_output}")
    
    if returncode == 0:
        print()
        print("✓ All sessions processed successfully!")
        print("✓ Date-aware deduplication applied:")
        print("    - Same date → shorter pattern preferred")
        print("    - Different dates → newest kept")
    else:
        print()
        print(f"⚠ Processing completed with return code: {returncode}")
        print("Check output above for details.")
    
    sys.exit(returncode)


if __name__ == "__main__":
    main()