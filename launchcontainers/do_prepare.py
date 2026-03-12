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

import logging
import os
import os.path as op

from bids import BIDSLayout

from launchcontainers import utils as do
from launchcontainers.prepare import prepare_dwi as prep_dwi

# import lc package utilities
logger = logging.getLogger("Launchcontainers")


def prepare_analysis_dir(parse_namespace, analysis_dir):
    """
    Initialize a prepared analysis directory from the user input files.

    This step copies the launchcontainers YAML file, the subject/session list,
    and the container-specific JSON file into the analysis directory. When
    Dask-based execution is enabled it also creates the worker log directory
    expected by later launch steps.

    Parameters
    ----------
    parse_namespace : argparse.Namespace
        Parsed CLI arguments for prepare mode.
    analysis_dir : str
        Target analysis directory created by :func:`launchcontainers.cli.create_analysis_dir`.

    Returns
    -------
    bool
        ``True`` when the basic analysis directory setup completes.
    """
    # read the yaml to get input info
    lc_config_fpath = parse_namespace.lc_config
    lc_config = do.read_yaml(lc_config_fpath)
    logger.info("\n prepare_analysis_dir reading lc config yaml")
    # read parameters from lc_config
    # the pipeline we are going to run
    container = lc_config["general"]["container"]
    # if force overwrite
    force = lc_config["general"]["force"]
    # if use dask to do the parallel, will abandon it in the future release
    use_dask = lc_config["general"]["use_dask"]

    # 2 create logdir for dask if use dask to launch
    if use_dask:
        host = lc_config["general"]["host"]
        jobqueue_config = lc_config["host_options"][host]
        daskworer_logdir = os.path.join(analysis_dir, "daskworker_log")

        if jobqueue_config["manager"] in ["sge", "slurm"] and not os.path.exists(
            daskworer_logdir
        ):
            os.makedirs(daskworer_logdir)
        if jobqueue_config["manager"] in ["local"]:
            if jobqueue_config["launch_mode"] == "dask_worker":
                os.makedirs(daskworer_logdir)
    else:
        logger.info("Not using dask to lauch task, no dask log dir")

    # 3 Copy the configs
    # define the potential exist config files
    # TODO: shall I add the time stamp to the file name?
    ana_dir_lcc = op.join(analysis_dir, "lc_config.yaml")
    ana_dir_ssl = op.join(analysis_dir, "subseslist.txt")
    container_configs_fname = f"{container}.json"
    ana_dir_cc = op.join(analysis_dir, container_configs_fname)

    # copy the config under the analysis folder
    do.copy_file(parse_namespace.lc_config, ana_dir_lcc, force)
    do.copy_file(parse_namespace.sub_ses_list, ana_dir_ssl, force)
    do.copy_file(
        parse_namespace.container_specific_config,
        ana_dir_cc,
        force,
    )

    # create a tmp dir to store all the launch script for SLURM and SGE

    logger.info(
        f"\n The analysis folder: {analysis_dir} successfully created,"
        "all the configs has been copied",
    )

    success = True
    return success


def main(parse_namespace, analysis_dir):
    """
    Run the full prepare workflow for one analysis directory.

    The workflow loads the BIDS dataset, filters the requested subject/session
    rows, copies the input configuration files into the analysis directory, and
    then delegates to the DWI preparation helpers to generate container-ready
    inputs for each selected session.

    Parameters
    ----------
    parse_namespace : argparse.Namespace
        Parsed CLI arguments for prepare mode.
    analysis_dir : str
        Prepared analysis directory for the current launch configuration.

    Returns
    -------
    bool
        ``True`` if both the analysis-level setup and the container-specific
        preparation complete successfully.
    """
    # read the yaml to get input info
    lc_config_fpath = parse_namespace.lc_config
    # read LC config yml
    lc_config = do.read_yaml(lc_config_fpath)
    print("\n cli.main() reading lc config yaml")
    # Get general information from the config.yaml file
    basedir = lc_config["general"]["basedir"]
    bidsdir_name = lc_config["general"]["bidsdir_name"]
    container = lc_config["general"]["container"]
    lc_config_fpath = parse_namespace.lc_config

    # read LC config yml
    lc_config = lc_config = do.read_yaml(lc_config_fpath)
    print("\n do_prepare reading lc config yaml")
    # Get general information from the config.yaml file
    basedir = lc_config["general"]["basedir"]
    bidsdir_name = lc_config["general"]["bidsdir_name"]
    # setup the subseslist read it into dataframe
    # get stuff from subseslist for future jobs scheduling
    sub_ses_list_path = parse_namespace.sub_ses_list
    df_subses, _ = do.read_df(sub_ses_list_path)
    if container in [
        "anatrois",
        "rtppreproc",
        "rtp-pipeline",
        "freesurferator",
        "rtp2-preproc",
        "rtp2-pipeline",
    ]:
        mask = (df_subses["RUN"] == "True") & (df_subses["dwi"] == "True")
    else:
        mask = df_subses["RUN"] == "True"
    df_subses = df_subses.loc[mask]

    # the prepare code
    # 1. setup analysis folder
    prepare_step1 = prepare_analysis_dir(parse_namespace, analysis_dir)

    # 2. do container specific preparation
    #   a. for DWI, prepare the container specific json
    #   b. create symbolic links
    logger.info("Reading the BIDS layout...")
    bids_dname = os.path.join(basedir, bidsdir_name)
    layout = BIDSLayout(bids_dname, validate=False)
    logger.info("finished reading the BIDS layout.")
    if container in [
        "anatrois",
        "rtppreproc",
        "rtp-pipeline",
        "freesurferator",
        "rtp2-preproc",
        "rtp2-pipeline",
    ]:
        logger.debug(f"{container} is in the list")

        prepare_step2 = prep_dwi.prepare_dwi(
            parse_namespace, analysis_dir, df_subses, layout
        )
    else:
        logger.error(f"{container} is not in the list")

    logger.critical(
        "\n#####\nAnalysis dir for run mode is \n" + f"{analysis_dir}\n",
    )
    return prepare_step1 and prepare_step2
