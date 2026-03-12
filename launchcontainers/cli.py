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

import argparse
import logging
import os
import os.path as op
import sys
from argparse import RawDescriptionHelpFormatter
from datetime import datetime

from launchcontainers import config_logger
from launchcontainers import do_launch
from launchcontainers import do_prepare
from launchcontainers import do_qc
from launchcontainers import utils as do
from launchcontainers.helper_function import copy_configs
from launchcontainers.helper_function import create_bids
from launchcontainers.helper_function import gen_subses
from launchcontainers.helper_function.zip_example_config import do_zip_configs

logger = logging.getLogger("Launchcontainers")


def get_parser():
    """
    Build and execute the top-level ``lc`` argument parser.

    The parser exposes the main launchcontainers workflows as subcommands:
    ``prepare``, ``run``, ``qc``, ``create_bids``, ``copy_configs``,
    ``zip_configs``, and ``gen_subses``. The parsed values are returned both
    as an ``argparse.Namespace`` and as a plain dictionary for legacy callers.

    Returns
    -------
    tuple[argparse.Namespace, dict]
        Parsed command-line values as both a namespace and a dictionary.
    """
    parser = argparse.ArgumentParser(
        prog="lc",
        description="""
        This python program helps you launch different neuroimaging pipelines on different
        computing clusters to enhance the reproducibility and reliability of data analysis.
        There are 2 main functionality: prepare folder structures and submit jobs.

        If you enter *lc prepare * you are in PREPARE mode, type lc prepare -h for flag help

        If you enter *lc run * you are in RUN mode, type lc run -h for flag help

        We have a another command line tools embedded with this software: \n

        checker: it is another tool that helps you check the integrity of the analysis
        TYPE checker --help for more details \n
        """,
        formatter_class=RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--log-dir",
        "-l",
        type=str,
        default=None,
        help="Directory to write lc.log and dask.log into (default: <workdir>/logs)",
    )
    subparsers = parser.add_subparsers(
        title="utilities",
        dest="mode",
        required=True,
        help="Launchcontainers functionalities",
    )

    # ------------------------
    # lc prepare
    # ------------------------
    prep = subparsers.add_parser(
        "prepare",
        help="Set up analysis folder structure",
    )
    prep.add_argument(
        "-lcc",
        "--lc_config",
        type=str,
        # default="",
        help="path to the config file",
    )
    prep.add_argument(
        "-ssl",
        "--sub_ses_list",
        type=str,
        # default="",
        help="path to the subSesList",
    )
    prep.add_argument(
        "-cc",
        "--container_specific_config",
        type=str,
        help="path to the container specific \
         config file, \
        it stores the parameters for the container.",
    )
    # ------------------------
    # lc run
    # ------------------------
    run = subparsers.add_parser(
        "run",
        help="Validate and submit jobs to cluster",
    )
    run.add_argument(
        "-w",
        "--workdir",
        required=True,
        type=str,
        help="Root of prepared analysis folders",
    )
    run.add_argument(
        "-R--run_lc",
        action="store_true",
        help="If not input, lc will just print commands without submitting;"
        "if you specify run_lc, it will launch the jobs",
    )
    # ------------------------
    # lc check
    # ------------------------
    qc = subparsers.add_parser(
        "qc",
        help="Validate the output of finished analysis, generate an QC report under analysis dir",
    )
    qc.add_argument(
        "-w",
        "--workdir",
        required=True,
        type=str,
        help="Root of finished analysis folders",
    )
    # ------------------------
    # lc create_bids
    # ------------------------
    create_bids = subparsers.add_parser(
        "create_bids",
        help="Create a fake bids folder based on subseslist and yml",
    )

    create_bids.add_argument(
        "-cbc",
        "--creat_bids_config",
        type=str,
        # default="",
        help="path to the create bids config file",
    )
    create_bids.add_argument(
        "-ssl",
        "--sub_ses_list",
        type=str,
        # default="",
        help="path to the subSesList",
    )
    # ------------------------
    # lc copy_configs
    # ------------------------
    copy_configs = subparsers.add_parser(
        "copy_configs",
        help="Copy example config files to working directory",
    )

    copy_configs.add_argument(
        "-o",
        "--output",
        type=str,
        help="Path to copy the configs, usually your working directory",
    )
    # ------------------------
    # lc zip_configs
    # ------------------------
    zip_configs = subparsers.add_parser(
        "zip_configs",
        help="Archive the example configs and store in the repo (for developer)",
    )

    zip_configs.set_defaults(func=lambda args: do_zip_configs())

    # ------------------------
    # lc gen_subseslist
    # ------------------------
    gen_subses = subparsers.add_parser(
        "gen_subses",
        help="generate subseslist on a given directory",
    )
    gen_subses.add_argument(
        "-b",
        "--basedir",
        type=str,
        help="Path to work, the directory contains sub and ses",
    )
    gen_subses.add_argument(
        "-n",
        "--name",
        type=str,
        help="output filename",
    )
    gen_subses.add_argument(
        "-o",
        "--output_dir",
        type=str,
        default="",
        help="Path to output the subses list, default is equal to basedir",
    )
    # Other optional arguements for lc

    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="if you want to open quiet mode, type --quiet, the the level will be critical",
    )
    # Other optional arguements for lc
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="if you want to open verbose mode, type --verbose, the the level will be info",
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="if you want to find out what is happening of particular step, \
            --type debug, this will print you more detailed information",
    )

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    parse_dict = vars(parser.parse_args())
    parse_namespace = parser.parse_args()

    return parse_namespace, parse_dict


def create_analysis_dir(parse_namespace):
    """
    Create the derivative container directory and analysis directory.

    The output location depends on the derivative layout requested in the
    launchcontainers YAML file. For the legacy layout an intermediate
    container-version directory is created and the actual analysis is stored in
    ``analysis-<analysis_name>``. For the newer layout the analysis directory
    is the container-version_<analysis_name> directory itself.

    Parameters
    ----------
    parse_namespace : argparse.Namespace
        Parsed CLI arguments containing at least ``lc_config``.

    Returns
    -------
    str
        Absolute path to the prepared analysis directory.
    """
    # read the yaml to get input info
    lc_config_fpath = parse_namespace.lc_config
    lc_config = do.read_yaml(lc_config_fpath)
    logger.info("\n setup_analysis_folder reading lc config yaml")
    # read parameters from lc_config
    basedir = lc_config["general"]["basedir"]
    bidsdir_name = lc_config["general"]["bidsdir_name"]
    deriv_layout = lc_config["general"]["deriv_layout"]
    # the pipeline we are going to run
    container = lc_config["general"]["container"]
    version = lc_config["container_specific"][container]["version"]
    analysis_name = lc_config["general"]["analysis_name"]

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
        print(
            f"Container layout is {deriv_layout}, creating folder at {container_folder}"
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


def main():
    """
    Dispatch the requested ``lc`` subcommand.

    This function configures logging, resolves the active analysis directory,
    and forwards execution to the matching helper module for prepare, run,
    quality-control, or utility workflows.
    """
    parse_namespace, parse_dict = get_parser()
    quiet = parse_namespace.quiet
    verbose = parse_namespace.verbose
    debug = parse_namespace.debug
    if parse_namespace.mode == "prepare":
        analysis_dir = create_analysis_dir(parse_namespace)
    elif parse_namespace.mode == "run":
        analysis_dir = parse_namespace.workdir
    logging_dir = parse_namespace.log_dir
    # define the analysis dir here to store the logging log of lc
    if not logging_dir:
        logging_dir = analysis_dir
    print(f" The logging dir is {logging_dir}")
    # get the dir and fpath for launchcontainer logger
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    if parse_namespace.mode == "prepare":
        logger.critical("\n....running prepare mode\n")
        logger.critical(f"Working on the dir: {analysis_dir}")
        logging_fname = f"lc_prepare_logger_{timestamp}"
        # set up the logger for prepare mode
        config_logger.setup_logger(quiet, verbose, debug, logging_dir, logging_fname)
        do_prepare.main(parse_namespace, analysis_dir)
    if parse_namespace.mode == "run":
        logger.critical("\n....running run mode\n")
        logging_fname = f"lc_run_logger_{timestamp}"
        # set up the logger for prepare mode
        config_logger.setup_logger(quiet, verbose, debug, logging_dir, logging_fname)
        do_launch.main(parse_namespace)
    if parse_namespace.mode == "qc":
        logger.critical("\n....running quality check mode\n")
        logging_fname = f"lc_qc_logger_{timestamp}"
        # set up the logger for prepare mode
        config_logger.setup_logger(quiet, verbose, debug, logging_dir, logging_fname)
        do_qc.main(parse_namespace)
    if parse_namespace.mode == "create_bids":
        create_bids.main()

    if parse_namespace.mode == "copy_configs":
        copy_configs.main()

    if parse_namespace.mode == "gen_subses":
        gen_subses.main()
    if parse_namespace.mode == "zip_configs":
        # launch the zip config function
        parse_namespace.func(parse_namespace)


if __name__ == "__main__":
    main()
