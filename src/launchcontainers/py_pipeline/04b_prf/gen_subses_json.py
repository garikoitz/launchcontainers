from __future__ import annotations

import json
import os

code_dir = '/scratch/tlei/soft/launchcontainers/src/launchcontainers/py_pipeline/04b_prf'
steps = ['prfprepare', 'prfanalyze-vista', 'prfresult']  # prfprepare #prfanalyze-vista #prfresult
tasks=["retRW","retFF","retCB"]
force=True
def gen_batch_json(subseslist_path, template_json, output_dir, step, tasks, force):
    # Load the template JSON
    with open(template_json) as f:
        template = json.load(f)

    # Read subject-session pairs from the text file
    with open(subseslist_path) as f:
        lines = f.readlines()[1:]  # skip the first line
    
    if not os.listdir(output_dir) or force:
        # if the pipeline is prfprepare or prfresult
        if step in ['prfprepare', 'prfresult']:
            # Generate JSONs for each subject-session pair
            for line in lines:
                sub, ses = line.strip('/t').split()

                # Replace placeholders in the template
                config = template.copy()
                config['subjects'] = f'{sub}'
                config['sessions'] = f'{ses}'
                config['tasks'] = ["retRW","retFF","retCB"],
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
                    sub, ses = line.strip('/t').split()

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

if __name__ == '__main__':
    force = True
    for step in steps:

        subseslist_path = os.path.join(code_dir, 'subseslist_votcloc.txt')
        output_dir = f'/scratch/tlei/VOTCLOC/BIDS/code/{step}_jsons'

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        template_json = os.path.join(code_dir, f'{step}.json')

        gen_batch_json(subseslist_path, template_json, output_dir, step, tasks,force)
