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
                config['tasks'] = ['retRW', 'retFF', 'retCB']
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


if __name__ == '__main__':

    # for bcbl /bcbl/home/public/Gari/VOTCLOC/main_exp
    # for dipc it is /scratch/tlei/VOTCLOC
    basedir = '/bcbl/home/public/Gari/VOTCLOC/main_exp'

    code_dir = os.path.join(basedir, 'code')
    # prfprepare #prfanalyze-vista #prfresult # 'prfprepare', 'prfanalyze-vista',
    steps = ['prfprepare', 'prfanalyze-vista', 'prfresult']
    tasks = ['retRW', 'retFF', 'retCB']
    force = True

    for step in steps:

        subseslist_path = os.path.join(code_dir, 'subseslist_prfnormal.txt')
        output_dir = os.path.join(code_dir , f'{step}_jsons')

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        template_json = os.path.join(code_dir, '04b_prf', f'{step}.json')

        gen_batch_json(subseslist_path, template_json, output_dir, step, tasks, force)
