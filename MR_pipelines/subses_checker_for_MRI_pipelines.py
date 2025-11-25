from __future__ import annotations

import json
import os
import re
from pathlib import Path


class BIDSProjectAnalyzer:
    def __init__(self, basedir):
        """
        Initialize the BIDS Project Analyzer with pipeline hierarchy knowledge

        Args:
            basedir (str): Base directory of the project
        """
        self.basedir = Path(basedir)

        # Base pipeline hierarchy (will be updated based on actual folders found)
        self.pipeline_hierarchy = {
            'dicom': {
                'level': 0,
                'parent': None,
                'children': ['BIDS'],
            },
            'BIDS': {
                'level': 1,
                'parent': 'dicom',
                'children': [],  # Will be populated with found fmriprep folders
            },
        }

        self.results = {
            'project_path': str(self.basedir),
            'folders_analyzed': {},
            'pipeline_hierarchy': self.pipeline_hierarchy,
            'analysis_folders': {},  # For tracking analysis-XX folders
            'options_references': {},  # For tracking options.json references
            'summary': {
                'all_subjects': set(),
                'all_sessions': set(),
                'missing_data': {},
            },
        }

    def find_bids_folders(self):
        """
        Find all BIDS-structured folders in the project according to hierarchy

        Returns:
            dict: Dictionary of folder paths and their types
        """
        folders = {}

        # Check dicom folder
        dicom_dir = self.basedir / 'dicom'
        if dicom_dir.exists():
            folders['dicom'] = str(dicom_dir)

        # Check BIDS folder
        bids_dir = self.basedir / 'BIDS'
        if bids_dir.exists():
            folders['BIDS'] = str(bids_dir)

        # Check derivatives folders
        derivatives_dir = self.basedir / 'BIDS' / 'derivatives'
        if derivatives_dir.exists():
            for pipeline_dir in derivatives_dir.iterdir():
                if pipeline_dir.is_dir():
                    pipeline_name = pipeline_dir.name

                    # Check if it's an fmriprep folder (pattern: fmriprep*)
                    if pipeline_name.startswith('fmriprep'):
                        folders[pipeline_name] = str(pipeline_dir)
                        # Update hierarchy to include this fmriprep folder
                        self.pipeline_hierarchy['BIDS']['children'].append(pipeline_name)
                        self.pipeline_hierarchy[pipeline_name] = {
                            'level': 2,
                            'parent': 'BIDS',
                            'children': [],
                        }

                    # Check if it's a prf* pipeline
                    elif pipeline_name.startswith('prf'):
                        # For prf* pipelines, we need to go one level deeper to find analysis folders
                        analysis_folders = self.find_analysis_folders(pipeline_dir)
                        if analysis_folders:
                            for analysis_name, analysis_path in analysis_folders.items():
                                full_name = f'{pipeline_name}/{analysis_name}'
                                folders[full_name] = analysis_path

                                # Store analysis folder info
                                if pipeline_name not in self.results['analysis_folders']:
                                    self.results['analysis_folders'][pipeline_name] = {}
                                self.results['analysis_folders'][pipeline_name][analysis_name] = analysis_path

                                # Read options.json to determine parent pipeline
                                parent_pipeline = self.get_parent_from_options(analysis_path)

                                # Add to hierarchy
                                self.pipeline_hierarchy[full_name] = {
                                    'level': self.get_pipeline_level(pipeline_name),
                                    'parent': parent_pipeline,
                                    'children': [],
                                }

                                # Update parent's children
                                if parent_pipeline and parent_pipeline in self.pipeline_hierarchy:
                                    if full_name not in self.pipeline_hierarchy[parent_pipeline]['children']:
                                        self.pipeline_hierarchy[parent_pipeline]['children'].append(
                                            full_name,
                                        )

                    # Handle other pipeline types
                    else:
                        folders[pipeline_name] = str(pipeline_dir)

        return folders

    def find_analysis_folders(self, pipeline_dir):
        """
        Find analysis-XX folders within a prf* pipeline directory

        Args:
            pipeline_dir (Path): Path to the pipeline directory

        Returns:
            dict: Dictionary of analysis folder names and paths
        """
        analysis_folders = {}

        for item in pipeline_dir.iterdir():
            if item.is_dir() and item.name.startswith('analysis-'):
                analysis_folders[item.name] = str(item)

        return analysis_folders

    def get_parent_from_options(self, analysis_path):
        """
        Read options.json file to determine the parent pipeline

        Args:
            analysis_path (str): Path to the analysis folder

        Returns:
            str: Name of the parent pipeline
        """
        options_file = Path(analysis_path) / 'options.json'

        if not options_file.exists():
            return None

        try:
            with open(options_file) as f:
                options = json.load(f)

            # Store the full options for reference
            self.results['options_references'][analysis_path] = options

            # Look for fmriprep reference
            options_str = json.dumps(options).lower()

            # Find fmriprep reference
            fmriprep_match = re.search(r'fmriprep[^"\s]*', options_str)
            if fmriprep_match:
                fmriprep_ref = fmriprep_match.group()
                # Find the actual fmriprep folder that matches this reference
                for folder_name in self.results['folders_analyzed'].keys():
                    if folder_name.startswith('fmriprep') and fmriprep_ref in folder_name.lower():
                        return folder_name
                # If no exact match, return the first fmriprep folder
                for folder_name in self.results['folders_analyzed'].keys():
                    if folder_name.startswith('fmriprep'):
                        return folder_name

            # Look for other pipeline references
            for pipeline in ['prfprepare', 'prfanalyze-vista']:
                if pipeline in options_str:
                    # Find the specific analysis folder
                    for folder_name in self.results['folders_analyzed'].keys():
                        if folder_name.startswith(pipeline):
                            return folder_name

        except (json.JSONDecodeError, Exception) as e:
            print(f'Warning: Could not read options.json from {analysis_path}: {e}')

        return None

    def get_pipeline_level(self, pipeline_name):
        """
        Determine the level of a pipeline in the hierarchy

        Args:
            pipeline_name (str): Name of the pipeline

        Returns:
            int: Level in the hierarchy
        """
        if pipeline_name.startswith('fmriprep'):
            return 2
        elif pipeline_name.startswith('prfprepare'):
            return 3
        elif pipeline_name.startswith('prfanalyze-vista'):
            return 4
        elif pipeline_name.startswith('prfresult'):
            return 5
        elif pipeline_name.startswith('l1_surface'):
            return 3
        else:
            return 999  # Unknown pipeline

    def extract_subjects_sessions(self, folder_path):
        """
        Extract subject and session information from a BIDS folder

        Args:
            folder_path (str): Path to the folder to analyze

        Returns:
            dict: Dictionary containing subjects, sessions, and sub-ses combinations
        """
        folder_path = Path(folder_path)
        subjects = set()
        sessions = set()
        sub_ses_combinations = set()

        # Find all subject directories
        for item in folder_path.iterdir():
            if item.is_dir() and item.name.startswith('sub-'):
                subject = item.name
                subjects.add(subject)

                # Check for session directories within subject
                session_dirs = [
                    d for d in item.iterdir() if d.is_dir()
                    and d.name.startswith('ses-')
                ]

                if session_dirs:
                    # Has sessions
                    for ses_dir in session_dirs:
                        session = ses_dir.name
                        sessions.add(session)
                        sub_ses_combinations.add((subject, session))
                else:
                    # No sessions (single-session study)
                    sub_ses_combinations.add((subject, None))

        return {
            'subjects': subjects,
            'sessions': sessions,
            'sub_ses_combinations': sub_ses_combinations,
            'has_sessions': len(sessions) > 0,
        }

    def analyze_project(self):
        """
        Analyze the entire project structure

        Returns:
            dict: Complete analysis results
        """
        print(f'Analyzing BIDS project at: {self.basedir}')
        print('=' * 60)

        # Find all BIDS folders
        folders = self.find_bids_folders()

        if not folders:
            print('No BIDS-structured folders found!')
            return self.results

        # Analyze each folder
        for folder_name, folder_path in folders.items():
            print(f'Analyzing: {folder_name}')
            folder_analysis = self.extract_subjects_sessions(folder_path)

            # Store results
            self.results['folders_analyzed'][folder_name] = {
                'path': folder_path,
                'subjects': sorted(list(folder_analysis['subjects'])),
                'sessions': sorted(list(folder_analysis['sessions'])),
                'sub_ses_combinations': sorted(list(folder_analysis['sub_ses_combinations'])),
                'has_sessions': folder_analysis['has_sessions'],
                'total_subjects': len(folder_analysis['subjects']),
                'total_sessions': len(folder_analysis['sessions']) if folder_analysis['has_sessions'] else 0,
                'total_combinations': len(folder_analysis['sub_ses_combinations']),
            }

            # Update global summary
            self.results['summary']['all_subjects'].update(folder_analysis['subjects'])
            self.results['summary']['all_sessions'].update(folder_analysis['sessions'])

        # Update hierarchy based on options.json references
        self.update_hierarchy_from_options()

        # Convert sets to sorted lists for JSON serialization
        self.results['summary']['all_subjects'] = sorted(
            list(self.results['summary']['all_subjects']),
        )
        self.results['summary']['all_sessions'] = sorted(
            list(self.results['summary']['all_sessions']),
        )

        return self.results

    def update_hierarchy_from_options(self):
        """
        Update the pipeline hierarchy based on options.json references
        """
        # Re-read options files now that all folders are analyzed
        for folder_name, folder_data in self.results['folders_analyzed'].items():
            if '/' in folder_name:  # This is a prf*/analysis-XX folder
                pipeline_name, analysis_name = folder_name.split('/', 1)
                analysis_path = folder_data['path']

                # Re-read options.json to get parent reference
                parent_pipeline = self.get_parent_from_options(analysis_path)

                if parent_pipeline:
                    self.pipeline_hierarchy[folder_name]['parent'] = parent_pipeline

                    # Update parent's children
                    if parent_pipeline in self.pipeline_hierarchy:
                        if folder_name not in self.pipeline_hierarchy[parent_pipeline]['children']:
                            self.pipeline_hierarchy[parent_pipeline]['children'].append(
                                folder_name,
                            )

    def print_initial_summary(self):
        """
        Print initial summary of dicom and BIDS folders
        """
        print('\n' + '=' * 60)
        print('INITIAL DATA SUMMARY')
        print('=' * 60)

        # Show dicom summary
        if 'dicom' in self.results['folders_analyzed']:
            dicom_data = self.results['folders_analyzed']['dicom']
            print('\n📦 DICOM (Original Data)')
            print(f"   Path: {dicom_data['path']}")
            print(f"   Total Subjects: {dicom_data['total_subjects']}")
            print(f"   Total Sessions: {dicom_data['total_sessions']}")
            print(f"   Total Sub-Ses combinations: {dicom_data['total_combinations']}")
        else:
            print('\n❌ DICOM folder not found!')

        # Show BIDS summary
        if 'BIDS' in self.results['folders_analyzed']:
            bids_data = self.results['folders_analyzed']['BIDS']
            print('\n📁 BIDS (Converted Data)')
            print(f"   Path: {bids_data['path']}")
            print(f"   Total Subjects: {bids_data['total_subjects']}")
            print(f"   Total Sessions: {bids_data['total_sessions']}")
            print(f"   Total Sub-Ses combinations: {bids_data['total_combinations']}")

            # Show conversion completeness
            if 'dicom' in self.results['folders_analyzed']:
                dicom_total = self.results['folders_analyzed']['dicom']['total_combinations']
                bids_total = bids_data['total_combinations']
                completion = (bids_total / dicom_total * 100) if dicom_total > 0 else 0
                print(f'   Conversion completion: {completion:.1f}% ({bids_total}/{dicom_total})')
        else:
            print('\n❌ BIDS folder not found!')

    def print_derivatives_summary(self):
        """
        Print summary of all derivatives folders
        """
        print('\n' + '=' * 60)
        print('DERIVATIVES SUMMARY')
        print('=' * 60)

        # Find derivatives folders
        derivatives = {}
        for folder_name, folder_data in self.results['folders_analyzed'].items():
            if folder_name not in ['dicom', 'BIDS'] and not folder_name.startswith('other_'):
                derivatives[folder_name] = folder_data

        if not derivatives:
            print('\n❌ No derivatives folders found!')
            return []

        # Group by pipeline type
        fmriprep_folders = {}
        prf_folders = {}
        other_folders = {}

        for folder_name, folder_data in derivatives.items():
            if folder_name.startswith('fmriprep'):
                fmriprep_folders[folder_name] = folder_data
            elif any(folder_name.startswith(f'prf{x}') for x in ['prepare', 'analyze-vista', 'result']):
                prf_folders[folder_name] = folder_data
            else:
                other_folders[folder_name] = folder_data

        # Print fMRIPrep folders
        if fmriprep_folders:
            print('\n🧠 fMRIPrep Pipelines:')
            for i, (folder_name, folder_data) in enumerate(fmriprep_folders.items(), 1):
                parent = self.pipeline_hierarchy.get(folder_name, {}).get('parent', 'BIDS')
                print(f'   {i}. 📊 {folder_name}')
                print(f"      Path: {folder_data['path']}")
                print(f'      Parent: {parent}')
                print(f"      Subjects: {folder_data['total_subjects']}")
                print(f"      Sessions: {folder_data['total_sessions']}")
                print(f"      Sub-Ses combinations: {folder_data['total_combinations']}")

                # Show completion relative to parent
                if parent and parent in self.results['folders_analyzed']:
                    parent_total = self.results['folders_analyzed'][parent]['total_combinations']
                    current_total = folder_data['total_combinations']
                    completion = (current_total / parent_total * 100) if parent_total > 0 else 0
                    print(
                        f'      Completion vs {parent}: {completion:.1f}% ({current_total}/{parent_total})',
                    )

        # Print PRF folders grouped by pipeline
        if prf_folders:
            print('\n🔬 PRF Analysis Pipelines:')

            # Group by base pipeline name
            prf_grouped = {}
            for folder_name, folder_data in prf_folders.items():
                if '/' in folder_name:
                    base_pipeline, analysis = folder_name.split('/', 1)
                    if base_pipeline not in prf_grouped:
                        prf_grouped[base_pipeline] = []
                    prf_grouped[base_pipeline].append((folder_name, folder_data))

            for base_pipeline, analyses in prf_grouped.items():
                print(f'\n   📋 {base_pipeline}:')
                for folder_name, folder_data in sorted(analyses):
                    parent = self.pipeline_hierarchy.get(folder_name, {}).get('parent', 'Unknown')
                    analysis_name = folder_name.split('/', 1)[1]

                    print(f'      • {analysis_name}')
                    print(f"        Path: {folder_data['path']}")
                    print(f'        Parent: {parent}')
                    print(f"        Subjects: {folder_data['total_subjects']}")
                    print(f"        Sub-Ses combinations: {folder_data['total_combinations']}")

                    # Show completion relative to parent
                    if parent and parent in self.results['folders_analyzed']:
                        parent_total = self.results['folders_analyzed'][parent]['total_combinations']
                        current_total = folder_data['total_combinations']
                        completion = (
                            current_total / parent_total
                            * 100
                        ) if parent_total > 0 else 0
                        print(
                            f'        Completion vs {parent}: {completion:.1f}% ({current_total}/{parent_total})',
                        )

        # Print other folders
        if other_folders:
            print('\n🔧 Other Pipelines:')
            for folder_name, folder_data in other_folders.items():
                parent = self.pipeline_hierarchy.get(folder_name, {}).get('parent', 'Unknown')
                print(f'   📊 {folder_name}')
                print(f"      Path: {folder_data['path']}")
                print(f'      Parent: {parent}')
                print(f"      Subjects: {folder_data['total_subjects']}")
                print(f"      Sub-Ses combinations: {folder_data['total_combinations']}")

        return list(derivatives.keys())

    def compare_folders(self, source_folder, target_folder):
        """
        Compare two folders and show missing data

        Args:
            source_folder (str): Source folder name
            target_folder (str): Target folder name
        """
        print('\n' + '=' * 60)
        print(f'COMPARING: {source_folder} → {target_folder}')
        print('=' * 60)

        if source_folder not in self.results['folders_analyzed']:
            print(f"❌ Source folder '{source_folder}' not found!")
            return

        if target_folder not in self.results['folders_analyzed']:
            print(f"❌ Target folder '{target_folder}' not found!")
            return

        source_data = self.results['folders_analyzed'][source_folder]
        target_data = self.results['folders_analyzed'][target_folder]

        source_combinations = set(source_data['sub_ses_combinations'])
        target_combinations = set(target_data['sub_ses_combinations'])

        missing_combinations = source_combinations - target_combinations
        extra_combinations = target_combinations - source_combinations

        # Summary statistics
        completion = (
            len(target_combinations) / len(source_combinations)
            * 100
        ) if source_combinations else 0

        print('\n📊 SUMMARY:')
        print(f'   Source ({source_folder}): {len(source_combinations)} combinations')
        print(f'   Target ({target_folder}): {len(target_combinations)} combinations')
        print(f'   Missing: {len(missing_combinations)} combinations')
        print(f'   Extra: {len(extra_combinations)} combinations')
        print(f'   Completion: {completion:.1f}%')

        # Show parent relationship from options.json if available
        if '/' in target_folder:
            target_path = target_data['path']
            if target_path in self.results['options_references']:
                print(f'   Options.json reference: Found in {target_path}')

        # Show missing combinations
        if missing_combinations:
            print(f'\n❌ MISSING in {target_folder}:')
            for combo in sorted(missing_combinations):
                if combo[1] is None:
                    print(f'   - {combo[0]}')
                else:
                    print(f'   - {combo[0]}/{combo[1]}')
        else:
            print('\n✅ No missing combinations!')

        # Show extra combinations
        if extra_combinations:
            print(f'\n⚠️  EXTRA in {target_folder}:')
            for combo in sorted(extra_combinations):
                if combo[1] is None:
                    print(f'   - {combo[0]}')
                else:
                    print(f'   - {combo[0]}/{combo[1]}')

        return {
            'missing': sorted(list(missing_combinations)),
            'extra': sorted(list(extra_combinations)),
            'completion': completion,
        }

    def interactive_comparison(self):
        """
        Interactive comparison interface
        """
        derivatives = self.print_derivatives_summary()

        if not derivatives:
            return

        print('\n' + '=' * 60)
        print('INTERACTIVE COMPARISON')
        print('=' * 60)

        while True:
            print('\nAvailable folders for comparison:')
            all_folders = ['dicom', 'BIDS'] + derivatives

            for i, folder in enumerate(all_folders, 1):
                if folder in self.results['folders_analyzed']:
                    count = self.results['folders_analyzed'][folder]['total_combinations']
                    print(f'   {i}. {folder} ({count} combinations)')

            print('\nOptions:')
            print("   - Enter two numbers to compare (e.g., '1 3' to compare folders 1 and 3)")
            print("   - Enter 'pipeline' to see pipeline hierarchy")
            print("   - Enter 'options' to see options.json references")
            print("   - Enter 'quit' to exit")

            choice = input('\nYour choice: ').strip()

            if choice.lower() == 'quit':
                break
            elif choice.lower() == 'pipeline':
                self.print_pipeline_hierarchy()
                continue
            elif choice.lower() == 'options':
                self.print_options_references()
                continue

            try:
                parts = choice.split()
                if len(parts) == 2:
                    idx1, idx2 = int(parts[0]) - 1, int(parts[1]) - 1
                    if 0 <= idx1 < len(all_folders) and 0 <= idx2 < len(all_folders):
                        source_folder = all_folders[idx1]
                        target_folder = all_folders[idx2]

                        if source_folder in self.results['folders_analyzed'] and target_folder in self.results['folders_analyzed']:
                            self.compare_folders(source_folder, target_folder)
                        else:
                            print('❌ One or both folders not found!')
                    else:
                        print('❌ Invalid folder numbers!')
                else:
                    print('❌ Please enter two numbers separated by space!')
            except ValueError:
                print("❌ Invalid input! Please enter numbers or 'quit'.")

    def print_pipeline_hierarchy(self):
        """
        Print the processing pipeline hierarchy
        """
        print('\n' + '=' * 60)
        print('PROCESSING PIPELINE HIERARCHY')
        print('=' * 60)

        def print_level(folder_name, level=0):
            indent = '   ' * level
            if folder_name in self.results['folders_analyzed']:
                count = self.results['folders_analyzed'][folder_name]['total_combinations']
                status = f'({count} combinations)'
            else:
                status = '(not found)'

            print(f'{indent}📁 {folder_name} {status}')

            # Print children
            if folder_name in self.pipeline_hierarchy:
                children = self.pipeline_hierarchy[folder_name]['children']
                for child in sorted(children):
                    print_level(child, level + 1)

        print_level('dicom')

    def print_options_references(self):
        """
        Print options.json references found in analysis folders
        """
        print('\n' + '=' * 60)
        print('OPTIONS.JSON REFERENCES')
        print('=' * 60)

        if not self.results['options_references']:
            print('\n❌ No options.json files found!')
            return

        for analysis_path, options in self.results['options_references'].items():
            folder_name = None
            for name, data in self.results['folders_analyzed'].items():
                if data['path'] == analysis_path:
                    folder_name = name
                    break

            print(f'\n📄 {folder_name or analysis_path}')
            print(f'   Path: {analysis_path}')

            # Look for pipeline references
            options_str = json.dumps(options).lower()

            if 'fmriprep' in options_str:
                fmriprep_matches = re.findall(r'fmriprep[^"\s]*', options_str)
                print(f"   fMRIPrep references: {', '.join(set(fmriprep_matches))}")

            if 'prfprepare' in options_str:
                print('   PRF Prepare reference found')

            if 'prfanalyze' in options_str:
                print('   PRF Analyze reference found')

    def save_results(self, output_file='bids_project_analysis.json'):
        """
        Save results to a JSON file

        Args:
            output_file (str): Output filename
        """
        # Convert tuples to lists for JSON serialization
        json_results = json.loads(json.dumps(self.results, default=str))

        with open(output_file, 'w') as f:
            json.dump(json_results, f, indent=2)

        print(f'\nDetailed results saved to: {output_file}')


def main():
    """
    Main function to run the BIDS project analysis
    """
    # Get project base directory
    basedir = input('Enter the path to your project base directory: ').strip()

    # Alternative: hardcode the path
    # basedir = "/path/to/your/project"

    if not os.path.exists(basedir):
        print(f"Error: Directory '{basedir}' does not exist!")
        return

    # Create analyzer and run analysis
    analyzer = BIDSProjectAnalyzer(basedir)
    results = analyzer.analyze_project()

    # Print initial summary (dicom and BIDS)
    analyzer.print_initial_summary()

    # Start interactive comparison
    analyzer.interactive_comparison()

    # Save results
    save_file = input('\nSave detailed results to JSON file? (y/n): ').strip().lower()
    if save_file in ['y', 'yes']:
        filename = input('Enter filename (default: bids_project_analysis.json): ').strip()
        if not filename:
            filename = 'bids_project_analysis.json'
        analyzer.save_results(filename)


if __name__ == '__main__':
    main()
