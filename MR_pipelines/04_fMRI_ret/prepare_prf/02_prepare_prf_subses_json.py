# """
# MIT License
# Copyright (c) 2024-2025 Yongning Lei
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or substantial
# portions of the Software.
# """
from __future__ import annotations

import json
import os


def gen_batch_json(subseslist_path, template_json, output_dir, step, tasks, force):
    # Load the template JSON
    with open(template_json) as f:
        template = json.load(f)

    # Read subject-session pairs from the text file
    with open(subseslist_path) as f:
        lines = f.readlines()[1:]  # skip the first line
        print(lines)
    if not os.listdir(output_dir) or force:
        # if the pipeline is prfprepare or prfresult
        if step in ['prfprepare']:
            # Generate JSONs for each subject-session pair
            for line in lines:
                # change the logic for the comma sep list
                parts = line.strip().split(',')
                sub , ses = parts[0], parts[1]
                # Replace placeholders in the template
                config = template.copy()
                config['subjects'] = f'{sub}'
                config['sessions'] = f'{ses}'
                config['tasks'] = ['all']
                # Save new JSON file
                json_filename = f'{output_dir}/all_sub-{sub}_ses-{ses}.json'
                with open(json_filename, 'w') as f:
                    json.dump(config, f, indent=4)

                print(f'Generated {json_filename}')
        elif step in ['prfresult']:
            # Generate JSONs for each subject-session pair
            for line in lines:
                # change the logic for the comma sep list
                parts = line.strip().split(',')
                sub , ses = parts[0], parts[1]
                # Replace placeholders in the template
                config = template.copy()
                config['subjects'] = f'{sub}'
                config['sessions'] = f'{ses}'
                config['tasks'] = ['all']
                # Save new JSON file
                json_filename = f'{output_dir}/all_sub-{sub}_ses-{ses}.json'
                with open(json_filename, 'w') as f:
                    json.dump(config, f, indent=4)

                print(f'Generated {json_filename}')
        # if the step is prfanalyze
        elif step in ['prfanalyze-vista']:
            for task in tasks:
                # Generate JSONs for each subject-session pair
                for line in lines:
                    # change the logic for the comma sep list
                    parts = line.strip().split(',')
                    sub , ses = parts[0], parts[1]
                    # Replace placeholders in the template
                    config = template.copy()
                    config['subjectName'] = f'{sub}'
                    config['sessionName'] = f'{ses}'
                    config['tasks'] = f'{task}'
                    # Save new JSON file
                    json_filename = f'{output_dir}/{task}_sub-{sub}_ses-{ses}.json'
                    with open(json_filename, 'w') as f:
                        json.dump(config, f, indent=4)

                    print(f'Generated {json_filename}')

def check_and_create_symlinks(template_json, basedir):
    """Check and create necessary symlinks for fmriprep analysis directory."""
    # Load the template JSON to get analysis name
    with open(template_json) as f:
        template = json.load(f)
    
    analysis_name = template['config']['fmriprep_analysis']
    
    # Define paths
    fmriprep_dir = os.path.join(basedir, 'BIDS', 'derivatives', 'fmriprep')
    analysis_dir = os.path.join(fmriprep_dir, f'analysis-{analysis_name}')
    source_dir = os.path.join(basedir, 'BIDS', 'derivatives', f'fmriprep-{analysis_name}')
    freesurfer_link = os.path.join(analysis_dir, 'sourcedata', 'freesurfer')
    
    # Check and create symlink for analysis directory
    if not os.path.exists(analysis_dir):
        if os.path.exists(source_dir):
            os.symlink(source_dir, analysis_dir)
            print(f'✓ Created symlink: {analysis_dir} -> {source_dir}')
        else:
            print(f'⚠ WARNING: Source directory does not exist: {source_dir}')
            return
    else:
        print(f'✓ Analysis directory exists: {analysis_dir}')
    
    # Check freesurfer directory/link
    if os.path.islink(freesurfer_link):
        if os.path.exists(freesurfer_link):
            print(f'✓ FreeSurfer link is valid: {freesurfer_link}')
        else:
            print(f'⚠ WARNING: FreeSurfer symlink is broken: {freesurfer_link}')
    elif os.path.exists(freesurfer_link):
        print(f'✓ FreeSurfer directory exists: {freesurfer_link}')
    else:
        print(f'⚠ WARNING: FreeSurfer directory/link does not exist: {freesurfer_link}')


if __name__ == '__main__':

    # for bcbl /bcbl/home/public/Gari/VOTCLOC/main_exp
    # for dipc it is /scratch/tlei/VOTCLOC
    basedir = '/scratch/tlei/VOTCLOC'
    code_dir = os.path.join(basedir, 'code')
    
    # example: prfprepare prfanalyze-vista prfresult 
    steps = [ 'prfanalyze-vista'] 
    force = True

    for step in steps:
        print(f'\n{"="*60}')
        print(f'STEP: {step}')
        print(f'{"="*60}')
        
        subseslist_path = os.path.join(code_dir, 'subseslist_ret_normal.txt')
        output_dir = os.path.join(code_dir, f'{step}_jsons')
        template_json = os.path.join(code_dir, '04b_prf', f'{step}.json')

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f'✓ Created output directory: {output_dir}')
        
        # Check and create symlinks for prfprepare
        if step == 'prfprepare':
            print(f'\nChecking fmriprep analysis setup...')
            check_and_create_symlinks(template_json, basedir)
        
        # Set tasks based on step
        if step not in ['prfanalyze-vista']:
            tasks = ['all']
        else:
            tasks = ['retRW', 'retFF', 'retCB', 'retfixRW', 'retfixFF',
                    'retfixRWblock', 'retfixRWblock01', 'retfixRWblock02']

        # Generate batch JSONs
        print(f'\nGenerating JSON files...')
        gen_batch_json(subseslist_path, template_json, output_dir, step, tasks, force)
        print(f'✓ Completed {step}')
