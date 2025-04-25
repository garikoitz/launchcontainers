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
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from bids import BIDSLayout
from dask.distributed import Client
from dask.distributed import LocalCluster


def move_html_figures(html_path, figures_path):
    '''
    from

    derivatives/fmriprep/sub-01/figures
    derivatives/fmriprep/sub-01.html

    to

    derivatives/fmriprep/fmriprep_report/sub-01/figures
    derivatives/fmriprep/fmriprep_report/sub-01.html

    '''
    output_dir = html_path.parent / 'fmriprep_report'
    os.makedirs(output_dir, exist_ok=True)

    src_html = html_path
    dest_html = output_dir / html_path.name

    src_figures = figures_path
    dest_figures = output_dir / figures_path.parent.name / figures_path.name
    os.makedirs(dest_figures, exist_ok=True)
    print(f'start to mv {src_html} and {src_figures} to the output_dir {output_dir}')
    try:
        # Use rsync to move files while preserving metadata
        subprocess.run(['rsync', '-av', str(src_html), str(dest_html)], check=True)
        subprocess.run(
            [
                'rsync', '-av', str(src_figures) + '/' ,
                str(dest_figures) + '/',
            ], check=True,
        )
    except Exception as e:
        print(f'when processing with {src_html.stem}, we have error {e}')


def main():
    basedir = '/bcbl/home/public/Gari/VOTCLOC/main_exp'
    bids_dir_name = 'BIDS'
    fmriprep_bidslayout = True
    fmriprep_analysis_name = 'runall_US'

    bids_dir = Path(basedir) / bids_dir_name

    if fmriprep_bidslayout:
        fmriprep_dir = Path('derivatives') / f'fmriprep-{fmriprep_analysis_name}'
    else:
        fmriprep_dir = Path('derivatives') / 'fmriprep' / f'analysis-{fmriprep_analysis_name}'

    # get all the subjects in the fmriprep derivatives dir
    layout = BIDSLayout(bids_dir, derivatives=True, validate=False)
    fmriprep_layout = layout.derivatives[str(fmriprep_dir)]

    subjects = fmriprep_layout.get_subjects()

    n_subjects = len(subjects)
    total_core = 16

    thread_per_worker = 2

    if n_subjects < total_core / thread_per_worker :
        n_workers = n_subjects
    else:
        n_workers = total_core / thread_per_worker

    # Set up Dask cluster with a thread pool
    # Adjust based on system
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=thread_per_worker)
    client = Client(cluster)

    # Submit tasks in parallel
    futures = [
        client.submit(
            move_html_figures, bids_dir / fmriprep_dir
            / f'sub-{sub}.html', bids_dir / fmriprep_dir / f'sub-{sub}' / 'figures',
        ) for sub in subjects
    ]

    # Collect results
    results = client.gather(futures)
    print(results)

    # Shutdown cluster after execution
    client.close()
    cluster.close()

    output_dir = bids_dir / fmriprep_dir / 'fmriprep_report'
    output_zip_path = str(output_dir)
    # Create a ZIP archive
    shutil.make_archive(output_zip_path, 'zip', output_dir)

    print('Zip file created successfully!')


if __name__ == '__main__':
    main()
