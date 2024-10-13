import subprocess
from pathlib import Path
import os
import zipfile
from importlib.metadata import version


def get_git_root():
    try:
        git_root = subprocess.check_output(['git', 'rev-parse', '--show-toplevel']).strip().decode('utf-8')
        return Path(git_root)
    except subprocess.CalledProcessError:
        return None



def zip_example_config():
    # Paths
    repo_dir = get_git_root()
    package_version = version('launchcontainers')
    example_configs_dir = os.path.join(repo_dir,'example_configs')
    output_zip = os.path.join(repo_dir,'src','launchcontainers' ,'configs', f'example_configs_{package_version}.zip')

    # Create the src/configs directory if it doesn't exist
    os.makedirs(os.path.dirname(output_zip), exist_ok=True)

    # Zip the folder
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(example_configs_dir):
            for file in files:
                filepath = os.path.join(root, file)
                arcname = os.path.relpath(filepath, example_configs_dir)
                zipf.write(filepath, arcname)
    print ("successfully zip your all your example configs into the package")
if __name__ == "__main__":
    zip_example_configs()