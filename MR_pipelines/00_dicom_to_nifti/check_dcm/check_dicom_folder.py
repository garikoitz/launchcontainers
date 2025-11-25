from __future__ import annotations

from pathlib import Path


def check_bids_dcm_files(bids_root_path):
    """
    Check BIDS folder structure for DCM files in fLoc and ret* folders

    Args:
        bids_root_path (str): Path to the BIDS root directory

    Returns:
        dict: Dictionary containing validation results
    """

    results = {
        'floc_issues': [],
        'ret_issues': [],
        'summary': {
            'total_subjects': 0,
            'total_sessions': 0,
            'floc_folders_checked': 0,
            'ret_folders_checked': 0,
        },
    }

    bids_root = Path(bids_root_path)

    if not bids_root.exists():
        print(f"Error: BIDS root directory '{bids_root_path}' does not exist!")
        return results

    # Find all subject directories
    subject_dirs = sorted([
        d for d in bids_root.iterdir()
        if d.is_dir() and d.name.startswith('sub-')
    ])

    print(f'Found {len(subject_dirs)} subject directories')
    results['summary']['total_subjects'] = len(subject_dirs)

    for sub_dir in subject_dirs:
        sub_id = sub_dir.name
        print(f'\nChecking subject: {sub_id}')

        # Find all session directories within this subject
        session_dirs = sorted([
            d for d in sub_dir.iterdir() if d.is_dir()
            and d.name.startswith('ses-')
        ])

        if not session_dirs:
            print(f'  No session directories found in {sub_id}')
            continue

        results['summary']['total_sessions'] += len(session_dirs)

        for ses_dir in session_dirs:
            ses_id = ses_dir.name
            print(f'  Checking session: {ses_id}')

            # Find all subdirectories in the session
            session_subdirs = [d for d in ses_dir.iterdir() if d.is_dir()]

            for subdir in session_subdirs:
                folder_name = subdir.name

                # Check for fLoc folders (case-insensitive)
                if 'floc' in folder_name.lower() and 'sbref' not in folder_name.lower():
                    results['summary']['floc_folders_checked'] += 1
                    dcm_files = list(subdir.glob('*.dcm'))
                    dcm_count = len(dcm_files)

                    print(f"    fLoc folder '{folder_name}': {dcm_count} DCM files")

                    if dcm_count != 160:
                        issue = {
                            'subject': sub_id,
                            'session': ses_id,
                            'folder': folder_name,
                            'expected_count': 160,
                            'actual_count': dcm_count,
                            'folder_path': str(subdir),
                        }
                        results['floc_issues'].append(issue)
                        print(f'    ⚠️  ISSUE: Expected 160 DCM files, found {dcm_count}')

                # Check for ret* folders (case-insensitive)
                elif folder_name.lower().startswith('ret') and 'sbref' not in folder_name.lower():
                    results['summary']['ret_folders_checked'] += 1
                    dcm_files = list(subdir.glob('*.dcm'))
                    dcm_count = len(dcm_files)

                    print(f"    ret* folder '{folder_name}': {dcm_count} DCM files")

                    if dcm_count != 156:
                        issue = {
                            'subject': sub_id,
                            'session': ses_id,
                            'folder': folder_name,
                            'expected_count': 156,
                            'actual_count': dcm_count,
                            'folder_path': str(subdir),
                        }
                        results['ret_issues'].append(issue)
                        print(f'    ⚠️  ISSUE: Expected 156 DCM files, found {dcm_count}')

    return results


def print_validation_report(results):
    """
    Print a formatted validation report

    Args:
        results (dict): Results from check_bids_dcm_files function
    """

    print('\n' + '=' * 60)
    print('BIDS DCM FILE VALIDATION REPORT')
    print('=' * 60)

    # Summary
    summary = results['summary']
    print('\nSUMMARY:')
    print(f"  Total subjects checked: {summary['total_subjects']}")
    print(f"  Total sessions checked: {summary['total_sessions']}")
    print(f"  fLoc folders checked: {summary['floc_folders_checked']}")
    print(f"  ret* folders checked: {summary['ret_folders_checked']}")

    # fLoc issues
    print(f"\nfLoc FOLDER ISSUES ({len(results['floc_issues'])} found):")
    if results['floc_issues']:
        for issue in results['floc_issues']:
            print(f"  ❌ {issue['subject']}/{issue['session']}/{issue['folder']}")
            print(f"     Expected: {issue['expected_count']}, Found: {issue['actual_count']}")
            print(f"     Path: {issue['folder_path']}")
    else:
        print('  ✅ No fLoc folder issues found!')

    # ret* issues
    print(f"\nret* FOLDER ISSUES ({len(results['ret_issues'])} found):")
    if results['ret_issues']:
        for issue in results['ret_issues']:
            print(f"  ❌ {issue['subject']}/{issue['session']}/{issue['folder']}")
            print(f"     Expected: {issue['expected_count']}, Found: {issue['actual_count']}")
            print(f"     Path: {issue['folder_path']}")
    else:
        print('  ✅ No ret* folder issues found!')

    print('\n' + '=' * 60)


def main():
    """
    Main function to run the BIDS DCM file validation
    """

    # Set your BIDS root directory path here
    bids_root_path = input('Enter the path to your BIDS root directory: ').strip()

    # Alternative: you can hardcode the path
    # bids_root_path = "/path/to/your/bids/dataset"

    print(f'Checking BIDS dataset at: {bids_root_path}')

    # Run the validation
    results = check_bids_dcm_files(bids_root_path)

    # Print the report
    print_validation_report(results)

    # Optionally save results to a file
    save_report = input('\nSave detailed report to file? (y/n): ').strip().lower()
    if save_report in ['y', 'yes']:
        import json
        report_file = 'bids_dcm_validation_report.json'
        with open(report_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f'Report saved to: {report_file}')


if __name__ == '__main__':
    main()
