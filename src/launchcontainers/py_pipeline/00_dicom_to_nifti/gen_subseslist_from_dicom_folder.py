from __future__ import annotations

import os

import typer


def generate_subses_list(
    base_dir: str,
    output_path: str,
    output_file: str = 'subseslist_Jan31.txt',

):
    """
    Generate a list of subject-session pairs from a directory structure.

    Args:
    - base_dir: The base directory to search for subject-session folders.
    - output_file: The name of the output file.
    """
    subses_pairs = []

    # Walk through the base directory
    for root, dirs, files in os.walk(base_dir):
        path_parts = root.split(os.sep)

        # Check if the path contains both a subject and a session
        if len(path_parts) >= 2:
            sub_folder = path_parts[-2]
            ses_folder = path_parts[-1]

            if sub_folder.startswith('sub-') and ses_folder.startswith('ses-'):
                # Extract subject and session IDs
                sub_id = sub_folder.split('-')[1]
                ses_id = ses_folder.split('-')[1]
                subses_pairs.append((sub_id, ses_id))

    # Determine the output path
    output_dir = os.path.abspath(output_path) if output_path else os.path.abspath(base_dir)

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Full path for the output file
    output_file_path = os.path.join(output_dir, output_file)

    # Write the results to the output file in TSV format
    with open(output_file_path, 'w') as f:
        f.write('sub\tses\n')  # Add a header line
        for sub_id, ses_id in sorted(subses_pairs):
            f.write(f"{sub_id}\t{ses_id}\n")

    print(f"subseslist.txt generated at {output_file_path}")


if __name__ == '__main__':
    typer.run(generate_subses_list)
