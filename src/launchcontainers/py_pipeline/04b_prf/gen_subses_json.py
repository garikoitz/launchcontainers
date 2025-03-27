from __future__ import annotations

import json
import os

code_dir = '/export/home/tlei/tlei/soft/launchcontainers/src/launchcontainers/py_pipeline/04b_prf'
steps = ['prfprepare', 'prfanalyze-vista', 'prfresult']  # prfprepare #prfanalyze-vista #prfresult


def gen_batch_json(subseslist_path, template_json, output_dir, step, force):
    # Load the template JSON
    with open(template_json) as f:
        template = json.load(f)

    # Read subject-session pairs from the text file
    with open(subseslist_path) as f:
        lines = f.readlines()[1:]  # skip the first line

    if not os.listdir(output_dir) or force:
        # Generate JSONs for each subject-session pair
        for line in lines:
            sub, ses = line.strip('/t').split()

            # Replace placeholders in the template
            config = template.copy()
            config['subjects'] = f'{sub}'
            config['sessions'] = f'{ses}'
            # Save new JSON file
            json_filename = f'{output_dir}/{step}_sub-{sub}_ses-{ses}.json'
            with open(json_filename, 'w') as f:
                json.dump(config, f, indent=4)

            print(f'Generated {json_filename}')


if __name__ == '__main__':
    force = True
    for step in steps:
        subseslist_path = os.path.join(code_dir, 'subseslist_votcloc.txt')
        output_dir = f'/bcbl/home/public/Gari/VOTCLOC/main_exp/BIDS/code/{step}_jsons'

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        template_json = os.path.join(code_dir, f'{step}.json')

        gen_batch_json(subseslist_path, template_json, output_dir, step, force)
