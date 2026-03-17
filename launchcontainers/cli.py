# """
# MIT License
# Copyright (c) 2020-2025 Garikoitz Lerma-Usabiaga
# Copyright (c) 2020-2022 Mengxing Liu
# Copyright (c) 2022-2023 Leandro Lecca
# Copyright (c) 2022-2025 Yongning Lei
# Copyright (c) 2023 David Linhardt
# Copyright (c) 2023 Iñigo Tellaetxe
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software
# and associated documentation files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to permit persons to
# whom the Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.
# """
from __future__ import annotations

import os
import os.path as op
import shutil
from argparse import Namespace
from datetime import datetime

import typer

from launchcontainers.log_setup import console, setup_verbosity, set_log_files

app = typer.Typer()


def create_analysis_dir(lc_config: str) -> str:
    """
    Create the derivative container directory and analysis directory.

    The output location depends on the derivative layout requested in the
    launchcontainers YAML file. For the legacy layout an intermediate
    container-version directory is created and the actual analysis is stored in
    ``analysis-<analysis_name>``. For the newer layout the analysis directory
    is the container-version_<analysis_name> directory itself.

    Parameters
    ----------
    lc_config : str
        Path to launchcontainers config YAML.

    Returns
    -------
    str
        Absolute path to the prepared analysis directory.
    """
    from launchcontainers import utils as do

    # read the yaml to get input info
    lc_config_fpath = lc_config
    lc_config_data = do.read_yaml(lc_config_fpath)
    console.print("\n setup_analysis_folder reading lc config yaml", style="cyan")
    # read parameters from lc_config
    basedir = lc_config_data["general"]["basedir"]
    bidsdir_name = lc_config_data["general"]["bidsdir_name"]
    deriv_layout = lc_config_data["general"]["deriv_layout"]
    # the pipeline we are going to run
    container = lc_config_data["general"]["container"]
    version = lc_config_data["container_specific"][container]["version"]
    analysis_name = lc_config_data["general"]["analysis_name"]

    # 1 create container dir and analysis dir
    if container in [
        "anatrois",
        "rtppreproc",
        "rtp-pipeline",
        "freesurferator",
        "rtp2-preproc",
        "rtp2-pipeline",
    ]:
        if deriv_layout == "legacy":
            container_folder = op.join(
                basedir,
                bidsdir_name,
                "derivatives",
                f"{container}-{version}",
            )
        else:
            container_folder = op.join(
                basedir,
                bidsdir_name,
                "derivatives",
                f"{container}-{version}_{analysis_name}",
            )
        # make dirs
        os.makedirs(container_folder, exist_ok=True)
        console.print(
            f"Container layout is {deriv_layout}, creating folder at {container_folder}",
            style="blue",
        )
        # 2 create analysis dir
        if deriv_layout == "legacy":
            analysis_dir = op.join(
                container_folder,
                f"analysis-{analysis_name}",
            )
        else:
            analysis_dir = container_folder
        os.makedirs(analysis_dir, exist_ok=True)

    return analysis_dir


@app.command()
def prepare(
    lc_config: str = typer.Option(
        ..., "--lc-config", "-lcc", help="Path to launchcontainers config YAML"
    ),
    sub_ses_list: str = typer.Option(
        ..., "--sub-ses-list", "-ssl", help="Path to subject/session list"
    ),
    container_specific_config: str = typer.Option(
        ...,
        "--container-specific-config",
        "-cc",
        help="Path to container-specific config",
    ),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Quiet mode"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose mode"),
    debug: bool = typer.Option(False, "--debug", "-d", help="Debug mode"),
):
    setup_verbosity(quiet=quiet, verbose=verbose, debug=debug)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    tmp_log_dir = op.join(".", "logs_tmp")
    os.makedirs(tmp_log_dir, exist_ok=True)
    log_fpath = op.join(tmp_log_dir, f"lc_prepare_{timestamp}.log")
    err_fpath = op.join(tmp_log_dir, f"lc_prepare_{timestamp}.err")
    set_log_files(log_fpath, err_fpath)
    analysis_dir = create_analysis_dir(lc_config)
    parse_namespace = Namespace(
        lc_config=lc_config,
        sub_ses_list=sub_ses_list,
        container_specific_config=container_specific_config,
    )
    from launchcontainers import do_prepare

    console.print(
        f"\n....running prepare mode\nWorking on the dir: {analysis_dir}",
        style="bold red",
    )
    do_prepare.main(parse_namespace, analysis_dir)
    # Copy logs to analysis_dir/prepare_log/ after prepare finishes
    prepare_log_dir = op.join(analysis_dir, "prepare_log")
    os.makedirs(prepare_log_dir, exist_ok=True)
    shutil.copy(log_fpath, op.join(prepare_log_dir, op.basename(log_fpath)))
    shutil.copy(err_fpath, op.join(prepare_log_dir, op.basename(err_fpath)))


@app.command()
def run(
    workdir: str = typer.Option(..., "--workdir", "-w", help="Working directory"),
    run_lc: bool = typer.Option(
        False, "--run-lc", "-R", help="Whether to run launchcontainers"
    ),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Quiet mode"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose mode"),
    debug: bool = typer.Option(False, "--debug", "-d", help="Debug mode"),
):
    setup_verbosity(quiet=quiet, verbose=verbose, debug=debug)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_log_dir = op.join(workdir, "run_log")
    os.makedirs(run_log_dir, exist_ok=True)
    set_log_files(
        op.join(run_log_dir, f"lc_run_{timestamp}.log"),
        op.join(run_log_dir, f"lc_run_{timestamp}.err"),
    )
    from launchcontainers import do_launch

    console.print("\n....running run mode\n", style="bold red")
    do_launch.main(workdir, run_lc)


@app.command()
def qc(
    workdir: str = typer.Option(..., "--workdir", "-w", help="Working directory"),
    log_dir: str = typer.Option(
        "./logs", "--log-dir", "-log", help="Directory to write logs"
    ),
    debug: bool = typer.Option(False, "--debug", "-d", help="Debug mode"),
):
    setup_verbosity(debug=debug)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    os.makedirs(log_dir, exist_ok=True)
    set_log_files(
        op.join(log_dir, f"qc_{timestamp}.log"),
        op.join(log_dir, f"qc_{timestamp}.err"),
    )
    from launchcontainers import do_qc

    console.print("\n....running quality check mode\n", style="bold red")
    do_qc.main(workdir, log_dir, debug)


# Add other commands similarly...


def main():
    app()


if __name__ == "__main__":
    main()
